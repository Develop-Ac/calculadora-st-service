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

      if (resp.status === 204) break; // sem documentos novos
      if (resp.status !== 200) {
        this.logger.warn(
          `ADN retornou status ${resp.status} (NSU ${ultimoNSU}): ${resp.rawBody.slice(0, 300)}`,
        );
        break;
      }

      if (i === 0 && resp.documentos[0]) {
        this.logger.debug(
          `Chaves do 1º DF-e retornado pela ADN: ${Object.keys(resp.documentos[0].raw).join(', ')}`,
        );
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
    page?: string;
    pageSize?: string;
  }) {
    const where: any = {};
    if (filtros.numero) where.numero = { contains: filtros.numero.trim() };
    if (filtros.cnpj) {
      const c = filtros.cnpj.replace(/\D/g, '');
      if (c) where.cnpj_prestador = { contains: c };
    }
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
      eventos: eventos.map((e) => ({ ...e, nsu: e.nsu.toString() })),
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
