import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { createHash } from 'crypto';
import { parseStringPromise } from 'xml2js';
import { PrismaService } from '../prisma/prisma.service';
import { NfseAdnClient, AdnDfeItem } from './nfse-adn.client';

/**
 * Distribuição de NFS-e do Padrão Nacional via ADN.
 *
 * Consumo por NSU (igual à distribuição de NF-e): ponteiro por CNPJ em
 * com_nfse_dist_controle; GET /DFe/{ultimoNSU} traz o próximo lote; avançamos
 * o NSU e repetimos até zerar o backlog ou bater o teto de iterações.
 *
 * Persistência: a NFS-e é a entidade (chave de acesso). Nota nova -> insere;
 * nota alterada (hash do XML mudou) -> atualiza; evento -> registra e ajusta a
 * `situacao` da NFS-e (ex.: CANCELADA).
 */
@Injectable()
export class NfseDistService {
  private readonly logger = new Logger(NfseDistService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly adn: NfseAdnClient,
  ) {}

  async sincronizar(): Promise<{
    novos: number;
    atualizados: number;
    eventos: number;
    ultimoNSU: number;
    maxNSU: number;
    iteracoes: number;
  }> {
    const cnpj = (process.env.NFSE_ADN_CNPJ || '').replace(/\D/g, '');
    if (!cnpj) throw new Error('NFSE_ADN_CNPJ não configurado (CNPJ a consultar na ADN).');

    const controle = await this.prisma.nfseDistControle.upsert({
      where: { cnpj },
      create: { cnpj },
      update: {},
    });

    let ultimoNSU = Number(controle.ultimo_nsu);
    let maxNSU = Number(controle.max_nsu);
    let novos = 0;
    let atualizados = 0;
    let eventos = 0;
    let iteracoes = 0;
    const maxLoops = Number(process.env.NFSE_DIST_MAX_LOOPS) || 20;

    for (let i = 0; i < maxLoops; i++) {
      iteracoes++;
      const resp = await this.adn.consultarDFe(ultimoNSU, cnpj);

      // Condição normal de "fim da fila" (404/E2220 ou 204): encerra sem alarde.
      if (resp.semDocumentos) break;
      if (resp.status !== 200) {
        const detalhe = resp.erros?.length ? JSON.stringify(resp.erros) : resp.rawBody.slice(0, 300);
        this.logger.warn(`ADN retornou status ${resp.status} (NSU ${ultimoNSU}): ${detalhe}`);
        break;
      }

      for (const doc of resp.documentos) {
        const r = await this.processar(cnpj, doc);
        if (r === 'novo') novos++;
        else if (r === 'atualizado') atualizados++;
        else if (r === 'evento') eventos++;
      }

      maxNSU = resp.maxNSU || maxNSU;
      const avanco = resp.ultimoNSU || ultimoNSU;
      if (avanco <= ultimoNSU && resp.documentos.length === 0) break;
      ultimoNSU = Math.max(ultimoNSU, avanco);

      await this.prisma.nfseDistControle.update({
        where: { cnpj },
        data: { ultimo_nsu: BigInt(ultimoNSU), max_nsu: BigInt(maxNSU), ultima_consulta: new Date() },
      });

      if (maxNSU && ultimoNSU >= maxNSU) break;
    }

    this.logger.log(
      `Distribuição NFS-e: ${novos} nova(s), ${atualizados} atualizada(s), ${eventos} evento(s); ` +
        `NSU ${ultimoNSU}/${maxNSU} em ${iteracoes} iteração(ões).`,
    );
    return { novos, atualizados, eventos, ultimoNSU, maxNSU, iteracoes };
  }

  /** Processa um DF-e: NFS-e (insere/atualiza) ou Evento (registra + ajusta situação). */
  private async processar(
    cnpjDest: string,
    doc: AdnDfeItem,
  ): Promise<'novo' | 'atualizado' | 'evento' | 'ignorado'> {
    const extra = await this.extrair(doc.xml).catch(() => ({}) as ExtractedNfse);
    const ehEvento = (doc.tipoDocumento || extra.tipo || '').toUpperCase().includes('EVENTO');

    if (ehEvento) {
      return (await this.persistirEvento(doc, extra)) ? 'evento' : 'ignorado';
    }
    return this.persistirNfse(cnpjDest, doc, extra);
  }

  private async persistirNfse(
    cnpjDest: string,
    doc: AdnDfeItem,
    extra: ExtractedNfse,
  ): Promise<'novo' | 'atualizado' | 'ignorado'> {
    const chave = doc.chaveAcesso || extra.chave;
    if (!chave) {
      this.logger.warn(`DF-e NSU ${doc.nsu} sem chave de acesso; ignorado.`);
      return 'ignorado';
    }
    const hash = createHash('sha256').update(doc.xml || '').digest('hex');
    const existente = await this.prisma.nfseDocumento.findUnique({ where: { chave_acesso: chave } });

    const dados = {
      cnpj_destinatario: cnpjDest,
      ultimo_nsu: BigInt(doc.nsu || 0),
      numero: extra.numero || null,
      serie: extra.serie || null,
      cnpj_prestador: extra.cnpjPrestador || null,
      nome_prestador: extra.nomePrestador || null,
      cnpj_tomador: extra.cnpjTomador || null,
      nome_tomador: extra.nomeTomador || null,
      valor: extra.valor ?? null,
      data_emissao: extra.dataEmissao || null,
      competencia: extra.competencia || null,
      papel: this.definirPapel(cnpjDest, extra),
      xml: doc.xml || '',
      hash,
      dados_json: extra.json ?? undefined,
    };

    if (!existente) {
      await this.prisma.nfseDocumento.create({ data: { chave_acesso: chave, ...dados } });
      return 'novo';
    }
    if (existente.hash === hash) return 'ignorado'; // nada mudou
    await this.prisma.nfseDocumento.update({ where: { chave_acesso: chave }, data: dados });
    return 'atualizado';
  }

  private async persistirEvento(doc: AdnDfeItem, extra: ExtractedNfse): Promise<boolean> {
    if (!doc.nsu) return false;
    const ja = await this.prisma.nfseEvento.findUnique({ where: { nsu: BigInt(doc.nsu) } });
    if (ja) return false;

    const chave = extra.chave || doc.chaveAcesso || '';
    await this.prisma.nfseEvento.create({
      data: {
        nsu: BigInt(doc.nsu),
        chave_acesso: chave,
        tipo_evento: extra.tipoEvento || null,
        data_evento: extra.dataEmissao || null,
        xml: doc.xml || '',
        dados_json: extra.json ?? undefined,
      },
    });

    // Reflete o evento na situação da NFS-e (cancelamento/substituição).
    if (chave) {
      const tipo = (extra.tipoEvento || '').toLowerCase();
      const situacao = tipo.includes('cancel')
        ? 'CANCELADA'
        : tipo.includes('substitu')
          ? 'SUBSTITUIDA'
          : null;
      if (situacao) {
        await this.prisma.nfseDocumento
          .update({ where: { chave_acesso: chave }, data: { situacao } })
          .catch(() => undefined); // NFS-e pode ainda não ter chegado
      }
    }
    return true;
  }

  private definirPapel(cnpjDest: string, extra: ExtractedNfse): string | null {
    const d = cnpjDest.replace(/\D/g, '');
    if (extra.cnpjTomador && extra.cnpjTomador.replace(/\D/g, '') === d) return 'TOMADOR';
    if (extra.cnpjPrestador && extra.cnpjPrestador.replace(/\D/g, '') === d) return 'PRESTADOR';
    return extra.papel || null;
  }

  /**
   * Extração best-effort do XML da NFS-e Nacional (leiaute infNFSe/emit/DPS/valores).
   * Tolerante a variações; guarda o JSON completo em dados_json p/ enriquecer depois.
   */
  private async extrair(xml: string): Promise<ExtractedNfse> {
    if (!xml) return {};
    const obj: any = await parseStringPromise(xml, {
      explicitArray: false,
      ignoreAttrs: false,
      tagNameProcessors: [(name) => name.replace(/^.*:/, '')], // remove namespace
    });

    const ehEvento = !!(obj?.evento || obj?.Evento || obj?.pedRegEvento || obj?.eventoNFSe);
    const root = obj?.NFSe || obj?.nfse || obj;
    const inf = root?.infNFSe || root?.InfNFSe || {};
    const emit = inf?.emit || inf?.prest || {};
    const dps = inf?.DPS?.infDPS || inf?.DPS || {};
    const toma = dps?.toma || dps?.tomador || {};
    const valores = inf?.valores || dps?.serv?.valores || {};

    const chave =
      this.limparChave(inf?.$?.Id) ||
      inf?.chNFSe ||
      root?.chNFSe ||
      this.buscarChaveEvento(obj) ||
      undefined;

    const valorRaw =
      valores?.vLiq ?? valores?.vServPrest?.vServ ?? valores?.vServ ?? valores?.vNF ?? undefined;
    const valor = valorRaw != null ? Number(String(valorRaw).replace(',', '.')) : undefined;

    return {
      tipo: ehEvento ? 'EVENTO' : 'NFSE',
      tipoEvento: ehEvento ? this.tipoEvento(obj) : undefined,
      chave: chave ? String(chave) : undefined,
      numero: inf?.nNFSe || inf?.numero || dps?.nDPS || undefined,
      serie: dps?.serie || inf?.serie || undefined,
      cnpjPrestador: emit?.CNPJ || emit?.cnpj || undefined,
      nomePrestador: emit?.xNome || emit?.nome || undefined,
      cnpjTomador: toma?.CNPJ || toma?.cnpj || undefined,
      nomeTomador: toma?.xNome || toma?.nome || undefined,
      valor: Number.isFinite(valor as number) ? (valor as number) : undefined,
      dataEmissao: this.parseData(inf?.dhProc || inf?.dhEmi || dps?.dhEmi),
      competencia: this.parseData(dps?.dCompet || inf?.dCompet),
      json: obj,
    };
  }

  private limparChave(id: any): string | undefined {
    if (!id) return undefined;
    const s = String(id).replace(/^NFS?e?/i, '').replace(/\D/g, '');
    return s.length >= 40 ? s : undefined;
  }

  private buscarChaveEvento(obj: any): string | undefined {
    // Procura recursivamente um campo com a chave da NFS-e referenciada.
    const alvo = ['chNFSe', 'chaveAcesso', 'ChaveAcesso'];
    const stack = [obj];
    while (stack.length) {
      const cur = stack.pop();
      if (!cur || typeof cur !== 'object') continue;
      for (const k of Object.keys(cur)) {
        if (alvo.includes(k) && typeof cur[k] === 'string') return cur[k];
        if (typeof cur[k] === 'object') stack.push(cur[k]);
      }
    }
    return undefined;
  }

  private tipoEvento(obj: any): string | undefined {
    const stack = [obj];
    while (stack.length) {
      const cur = stack.pop();
      if (!cur || typeof cur !== 'object') continue;
      for (const k of Object.keys(cur)) {
        if (/tipoEvento|tpEvento|xMotivo|descEvento/i.test(k) && cur[k]) return String(cur[k]);
        if (typeof cur[k] === 'object') stack.push(cur[k]);
      }
    }
    return undefined;
  }

  private parseData(v: any): Date | undefined {
    if (!v) return undefined;
    const d = new Date(String(v));
    return isNaN(d.getTime()) ? undefined : d;
  }

  // -------- Consultas p/ o frontend --------

  async listar(filtros: {
    numero?: string;
    cnpj?: string;
    dataInicio?: string;
    dataFim?: string;
    papel?: string;
    page?: string;
    pageSize?: string;
  }) {
    const where: any = {};
    const root = (process.env.NFSE_ADN_CNPJ || '').replace(/\D/g, '').slice(0, 8);
    const prestados = (filtros.papel || '').toUpperCase() === 'PRESTADOS';
    const c = (filtros.cnpj || '').replace(/\D/g, '');

    // Tomados (padrão): minha empresa é o TOMADOR; contraparte filtrada = prestador.
    // Prestados: minha empresa é o PRESTADOR; contraparte filtrada = tomador.
    // Casa por RAIZ do CNPJ (8 díg.) p/ cobrir filiais do grupo.
    if (prestados) {
      if (root) where.cnpj_prestador = { startsWith: root };
      if (c) where.cnpj_tomador = { contains: c };
    } else {
      if (root) where.cnpj_tomador = { startsWith: root };
      if (c) where.cnpj_prestador = { contains: c };
    }

    if (filtros.numero) where.numero = { contains: filtros.numero.trim() };
    if (filtros.dataInicio || filtros.dataFim) {
      where.data_emissao = {};
      if (filtros.dataInicio) where.data_emissao.gte = new Date(`${filtros.dataInicio}T00:00:00`);
      if (filtros.dataFim) where.data_emissao.lte = new Date(`${filtros.dataFim}T23:59:59`);
    }

    const page = Math.max(1, Number(filtros.page) || 1);
    const pageSize = Math.min(Math.max(Number(filtros.pageSize) || 50, 1), 500);

    const [total, rows] = await this.prisma.$transaction([
      this.prisma.nfseDocumento.count({ where }),
      this.prisma.nfseDocumento.findMany({
        where,
        orderBy: { data_emissao: 'desc' },
        skip: (page - 1) * pageSize,
        take: pageSize,
      }),
    ]);

    return { total, page, pageSize, items: rows.map((r) => this.serializar(r)) };
  }

  async detalhe(chave: string) {
    const doc = await this.prisma.nfseDocumento.findUnique({ where: { chave_acesso: chave } });
    if (!doc) throw new NotFoundException(`NFS-e não encontrada: ${chave}`);
    const eventos = await this.prisma.nfseEvento.findMany({
      where: { chave_acesso: chave },
      orderBy: { nsu: 'asc' },
    });
    return {
      ...this.serializar(doc),
      xml: doc.xml,
      detalhado: await this.detalhar(doc.xml).catch(() => null),
      eventos: eventos.map((e) => ({ ...e, nsu: e.nsu.toString() })),
    };
  }

  /** Extrai a NFS-e em blocos estruturados (partes, serviço, valores e IMPOSTOS). */
  private async detalhar(xml: string): Promise<any> {
    if (!xml) return null;
    const obj: any = await parseStringPromise(xml, {
      explicitArray: false,
      ignoreAttrs: false,
      tagNameProcessors: [(name) => name.replace(/^.*:/, '')],
    });

    const root = obj?.NFSe || obj?.nfse || obj;
    const inf = root?.infNFSe || root?.InfNFSe || {};
    const emit = inf?.emit || {};
    const valN = inf?.valores || {}; // valores efetivos da NFS-e (vBC, pAliqAplic, vISSQN, vLiq)
    const dps = inf?.DPS?.infDPS || inf?.DPS || {};
    const prest = dps?.prest || {};
    const toma = dps?.toma || {};
    const serv = dps?.serv || {};
    const cServ = serv?.cServ || {};
    const valD = dps?.valores || {}; // valores declarados na DPS (trib, totTrib)
    const trib = valD?.trib || {};
    const tribMun = trib?.tribMun || {};
    const tribFed = trib?.tribFed || {};
    const pisCofins = tribFed?.piscofins || tribFed?.PisCofins || {};
    const totTrib = trib?.totTrib?.vTotTrib || {};

    const n = (v: any): number | null => {
      if (v == null) return null;
      const x = Number(String(v).replace(',', '.'));
      return Number.isFinite(x) ? x : null;
    };
    const ender = (e: any) =>
      e
        ? {
            logradouro: e.xLgr,
            numero: e.nro,
            complemento: e.xCpl,
            bairro: e.xBairro,
            municipio: e.enderNac?.cMun || e.endNac?.cMun || e.cMun,
            uf: e.UF || e.enderNac?.UF,
            cep: e.CEP || e.enderNac?.CEP || e.endNac?.CEP,
          }
        : null;

    const TRIB_ISSQN: Record<string, string> = {
      '1': 'Operação tributável',
      '2': 'Exportação de serviços',
      '3': 'Não incidência',
      '4': 'Imunidade',
      '5': 'Exigibilidade suspensa (judicial)',
      '6': 'Exigibilidade suspensa (administrativa)',
    };
    const RET_ISSQN: Record<string, string> = {
      '1': 'Não retido',
      '2': 'Retido pelo tomador',
      '3': 'Retido pelo intermediário',
    };
    const SIMPLES: Record<string, string> = {
      '1': 'Não optante',
      '2': 'Optante - MEI',
      '3': 'Optante - ME/EPP',
    };

    return {
      identificacao: {
        numero: inf?.nNFSe,
        serie: dps?.serie,
        dhProcessamento: inf?.dhProc,
        dhEmissao: dps?.dhEmi,
        competencia: dps?.dCompet,
        situacaoCodigo: inf?.cStat,
        nDFSe: inf?.nDFSe,
        localEmissao: inf?.xLocEmi,
        localPrestacao: inf?.xLocPrestacao,
        localIncidencia: inf?.xLocIncid,
        codTribNacional: cServ?.cTribNac,
        descTribNacional: inf?.xTribNac,
      },
      prestador: {
        cnpj: emit?.CNPJ || emit?.CPF,
        inscricaoMunicipal: emit?.IM,
        nome: emit?.xNome,
        fone: emit?.fone,
        email: emit?.email,
        endereco: ender(emit?.enderNac ? emit : null) || ender(emit),
        simplesNacional: SIMPLES[String(prest?.regTrib?.opSimpNac)] || prest?.regTrib?.opSimpNac,
        regimeEspecial: prest?.regTrib?.regEspTrib,
      },
      tomador: {
        cnpj: toma?.CNPJ || toma?.CPF,
        nome: toma?.xNome,
        fone: toma?.fone,
        email: toma?.email,
        endereco: ender(toma?.end),
      },
      servico: {
        codTribNacional: cServ?.cTribNac,
        descricao: cServ?.xDescServ,
        cNBS: cServ?.cNBS,
        codLocalPrestacao: serv?.locPrest?.cLocPrestacao,
      },
      valores: {
        valorServico: n(valD?.vServPrest?.vServ) ?? n(valN?.vServ),
        baseCalculo: n(valN?.vBC),
        aliquota: n(valN?.pAliqAplic),
        valorIssqn: n(valN?.vISSQN),
        valorLiquido: n(valN?.vLiq),
        descontoIncondicionado: n(valD?.vServPrest?.vDescCondIncond?.vDescIncond),
        descontoCondicionado: n(valD?.vServPrest?.vDescCondIncond?.vDescCond),
      },
      impostos: {
        issqn: {
          tributacao: TRIB_ISSQN[String(tribMun?.tribISSQN)] || tribMun?.tribISSQN,
          retencao: RET_ISSQN[String(tribMun?.tpRetISSQN)] || tribMun?.tpRetISSQN,
          baseCalculo: n(valN?.vBC),
          aliquota: n(valN?.pAliqAplic),
          valor: n(valN?.vISSQN),
          municipioIncidencia: inf?.xLocIncid,
        },
        federais: {
          pis: n(pisCofins?.vPis ?? tribFed?.vPis),
          cofins: n(pisCofins?.vCofins ?? tribFed?.vCofins),
          irrf: n(tribFed?.vRetIRRF ?? tribFed?.vIRRF),
          csll: n(tribFed?.vRetCSLL ?? tribFed?.vCSLL),
          inss: n(tribFed?.vRetCP ?? tribFed?.vINSS ?? tribFed?.vCP),
        },
        totalTributos: {
          federal: n(totTrib?.vTotTribFed),
          estadual: n(totTrib?.vTotTribEst),
          municipal: n(totTrib?.vTotTribMun),
        },
      },
    };
  }

  async danfse(chave: string): Promise<Buffer> {
    const doc = await this.prisma.nfseDocumento.findUnique({ where: { chave_acesso: chave } });
    const cnpj = doc?.cnpj_destinatario || process.env.NFSE_ADN_CNPJ;
    return this.adn.baixarDanfse(chave, cnpj || undefined);
  }

  async eventos(chave: string) {
    const r = await this.adn.consultarEventos(chave);
    return { status: r.status, total: r.documentos.length, eventos: r.documentos };
  }

  /** Remove campos pesados (xml) e serializa BigInt p/ a listagem. */
  private serializar(r: any) {
    const { xml, ...rest } = r;
    return { ...rest, ultimo_nsu: r.ultimo_nsu?.toString() };
  }
}

interface ExtractedNfse {
  tipo?: string;
  tipoEvento?: string;
  chave?: string;
  numero?: string;
  serie?: string;
  cnpjPrestador?: string;
  nomePrestador?: string;
  cnpjTomador?: string;
  nomeTomador?: string;
  valor?: number;
  dataEmissao?: Date;
  competencia?: Date;
  papel?: string;
  json?: any;
}
