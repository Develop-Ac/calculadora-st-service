/**
 * Parser do XML do CT-e (modelo 57, layout cteProc/CTe/infCte v4.00) → CteData.
 * Usa xml2js (mesma lib do módulo icms). Tolerante a campos ausentes.
 */
import * as xml2js from 'xml2js';
import { CteData, CtePartelE, CteComponente } from './cte.types';

const PARSER = new xml2js.Parser({
  explicitArray: false,
  ignoreAttrs: false,
  tagNameProcessors: [xml2js.processors.stripPrefix],
});

/** Sempre devolve array (xml2js entrega objeto único quando há só 1 elemento). */
function asArray<T>(v: T | T[] | undefined | null): T[] {
  if (v == null) return [];
  return Array.isArray(v) ? v : [v];
}

function txt(v: any): string {
  if (v == null) return '';
  if (typeof v === 'object') {
    // xml2js pode devolver { _: 'texto', $: {...} }
    if (typeof v._ === 'string') return v._.trim();
    return '';
  }
  return String(v).trim();
}

function num(v: any): number {
  const raw = txt(v).replace(',', '.');
  const n = Number.parseFloat(raw);
  return Number.isFinite(n) ? n : 0;
}

const MODAIS: Record<string, string> = {
  '01': 'RODOVIÁRIO',
  '02': 'AÉREO',
  '03': 'AQUAVIÁRIO',
  '04': 'FERROVIÁRIO',
  '05': 'DUTOVIÁRIO',
  '06': 'MULTIMODAL',
};

const TP_CTE: Record<string, string> = {
  '0': 'NORMAL',
  '1': 'COMPLEMENTO DE VALORES',
  '2': 'ANULAÇÃO',
  '3': 'SUBSTITUTO',
};

const TP_SERV: Record<string, string> = {
  '0': 'NORMAL',
  '1': 'SUBCONTRATAÇÃO',
  '2': 'REDESPACHO',
  '3': 'REDESPACHO INTERMEDIÁRIO',
  '4': 'SERVIÇO VINCULADO A MULTIMODAL',
};

const TOMADOR: Record<string, string> = {
  '0': 'REMETENTE',
  '1': 'EXPEDIDOR',
  '2': 'RECEBEDOR',
  '3': 'DESTINATÁRIO',
  '4': 'OUTROS',
};

// CST do ICMS no CT-e → descrição da situação tributária (texto do DACTE).
const CST_ICMS: Record<string, string> = {
  '00': 'TRIBUTAÇÃO NORMAL',
  '20': 'BC REDUZIDA',
  '40': 'ICMS ISENTO',
  '41': 'ICMS NÃO TRIBUTADO',
  '45': 'ISENTO/NÃO TRIB./DIFERIDO',
  '51': 'DIFERIDO',
  '60': 'ICMS COBRADO POR ST',
  '90': 'OUTROS',
};

function montarEndereco(ender: any): string {
  if (!ender) return '';
  const lgr = txt(ender.xLgr);
  const nro = txt(ender.nro);
  const cpl = txt(ender.xCpl);
  const bairro = txt(ender.xBairro);
  let s = lgr;
  if (nro) s += (s ? ', ' : '') + nro;
  if (cpl) s += (s ? ' ' : '') + cpl;
  if (bairro) s += (s ? ' - ' : '') + bairro;
  return s;
}

/** Lê um bloco de parte (emit/rem/dest/exped/receb) com seu sub-bloco de endereço. */
function parseParte(bloco: any, enderKey: string): CtePartelE {
  if (!bloco) {
    return { nome: '', cnpjCpf: '', ie: '', endereco: '', municipio: '', uf: '', cep: '', fone: '' };
  }
  const ender = bloco[enderKey] || {};
  return {
    nome: txt(bloco.xNome),
    cnpjCpf: txt(bloco.CNPJ) || txt(bloco.CPF),
    ie: txt(bloco.IE),
    endereco: montarEndereco(ender),
    municipio: txt(ender.xMun),
    uf: txt(ender.UF),
    cep: txt(ender.CEP),
    fone: txt(bloco.fone) || txt(ender.fone),
  };
}

/** Encontra o grupo de ICMS presente (ICMS00/20/45/60/90/OutraUF/SN) e extrai CST/valores. */
function parseIcms(imp: any): { cst: string; vBC: number; pICMS: number; vICMS: number } {
  const icms = imp?.ICMS;
  const out = { cst: '', vBC: 0, pICMS: 0, vICMS: 0 };
  if (!icms) return out;
  // Pega o primeiro filho que for objeto (ICMS00, ICMS60, ICMSSN, etc.)
  for (const key of Object.keys(icms)) {
    const grupo = (icms as any)[key];
    if (grupo && typeof grupo === 'object') {
      out.cst = txt(grupo.CST) || txt(grupo.CSOSN) || (key.replace(/^ICMS/, '') || '');
      out.vBC = num(grupo.vBC);
      out.pICMS = num(grupo.pICMS);
      out.vICMS = num(grupo.vICMS);
      return out;
    }
  }
  return out;
}

export function parseCteXml(xml: string): CteData {
  let root: any;
  try {
    // xml2js.parseStringSync não existe; usamos o parser síncrono via processamento direto.
    // Fazemos parse síncrono com um truque: parseString é callback, mas resolve imediato.
    let parsed: any = null;
    PARSER.parseString(xml, (err, result) => {
      if (err) throw err;
      parsed = result;
    });
    root = parsed;
  } catch {
    root = null;
  }

  const empty: CteData = vazio();
  if (!root) return empty;

  // cteProc > CTe > infCte  (ou direto CTe > infCte)
  const cte = root.cteProc?.CTe || root.CTe || root.cteProc?.cte || null;
  const infCte = cte?.infCte;
  if (!infCte) return empty;

  const ide = infCte.ide || {};
  const emitBloco = infCte.emit || {};
  const norm = infCte.infCTeNorm || {};
  const imp = infCte.imp || {};
  const vPrest = infCte.vPrest || {};
  const infCarga = norm.infCarga || {};
  const proto = root.cteProc?.protCTe?.infProt || root.protCTe?.infProt || {};

  // Chave: do Id do infCte (CTe + 44 díg) ou da protCTe
  const idAttr = txt(infCte.$?.Id).replace(/^CTe/i, '');
  const chave = (idAttr && idAttr.length >= 44 ? idAttr.slice(-44) : '') || txt(proto.chCTe);

  const icms = parseIcms(imp);

  // Documentos transportados (NF-e)
  const infDocs = asArray(norm.infDoc);
  const documentosNFe: string[] = [];
  for (const d of infDocs) {
    for (const nfe of asArray(d?.infNFe)) {
      const ch = txt(nfe?.chave);
      if (ch) documentosNFe.push(ch);
    }
  }

  // Componentes do frete
  const componentes: CteComponente[] = asArray(vPrest.Comp)
    .map((c) => ({ nome: txt(c?.xNome), valor: num(c?.vComp) }))
    .filter((c) => c.nome || c.valor);

  // Tomador
  const toma3 = txt(ide.toma3?.toma);
  const toma4 = ide.toma4 ? '4' : '';
  const tomadorCod = toma3 || toma4;

  // Partes (resolvidas para uso no DACTE e na tela de Detalhe)
  const remetente = parseParte(infCte.rem, 'enderReme');
  const destinatario = parseParte(infCte.dest, 'enderDest');
  const expedidor = infCte.exped ? parseParte(infCte.exped, 'enderExped') : null;
  const recebedor = infCte.receb ? parseParte(infCte.receb, 'enderReceb') : null;

  // Tomador como PARTE: 0=Remetente,1=Expedidor,2=Recebedor,3=Destinatário,4=Outros(toma4).
  let tomadorParte: CtePartelE | null = null;
  if (tomadorCod === '0') tomadorParte = remetente;
  else if (tomadorCod === '1') tomadorParte = expedidor;
  else if (tomadorCod === '2') tomadorParte = recebedor;
  else if (tomadorCod === '3') tomadorParte = destinatario;
  else if (tomadorCod === '4') tomadorParte = parseParte(ide.toma4, 'enderToma');

  // RNTRC (modal rodoviário)
  const rntrc = txt(norm.infModal?.rodo?.RNTRC);

  // Emitente (transportadora). "EMITIDO POR" no DACTE = a própria transportadora emitente.
  const emitente = parseParte(emitBloco, 'enderEmit');

  // ---- Quantidades da carga (infQ por tpMed) ----
  const infQ = asArray(infCarga.infQ);
  const qPorMedida = (...alvos: string[]): number => {
    const item = infQ.find((q) => {
      const m = txt(q?.tpMed).toUpperCase();
      return alvos.some((a) => m.includes(a));
    });
    return item ? num(item.qCarga) : 0;
  };

  // ---- Reforma tributária (IBS/CBS) ----
  const g = imp.IBSCBS?.gIBSCBS || {};

  // ---- QR Code (infCTeSupl) ----
  const qrCodeUrl = txt(cte.infCTeSupl?.qrCodCTe);

  // ---- ObsCont (pedido, rota, tipo mercadoria) ----
  const obsConts = asArray(infCte.compl?.ObsCont);
  const obsTextos = obsConts.map((o) => txt(o?.xTexto)).join(' ');
  const acharNoObs = (regex: RegExp): string => {
    const m = obsTextos.match(regex);
    return m ? m[1].trim() : '';
  };
  const pedido = acharNoObs(/N\s*PEDIDO:\s*([0-9.\-]+)/i);
  const rota = acharNoObs(/ROTA:\s*([^\-.]+)/i);
  const especie = acharNoObs(/TIPO\s*MERCAD:\s*([^.\-]+)/i);

  return {
    chave,
    numero: txt(ide.nCT),
    serie: txt(ide.serie),
    modelo: txt(ide.mod) || '57',
    modal: MODAIS[txt(ide.modal)] || txt(ide.modal),
    cfop: txt(ide.CFOP),
    naturezaOperacao: txt(ide.natOp),
    tipoCte: TP_CTE[txt(ide.tpCTe)] || '',
    tipoServico: TP_SERV[txt(ide.tpServ)] || '',
    tomador: TOMADOR[tomadorCod] || '',
    emitidoPor: emitente.nome,
    dataEmissao: txt(ide.dhEmi),

    protocolo: txt(proto.nProt),
    dataAutorizacao: txt(proto.dhRecbto),

    origemMunicipio: txt(ide.xMunIni),
    origemUf: txt(ide.UFIni),
    destinoMunicipio: txt(ide.xMunFim),
    destinoUf: txt(ide.UFFim),

    emitente,
    rntrc,
    remetente,
    destinatario,
    expedidor,
    recebedor,
    tomadorParte,

    valorTotalPrestacao: num(vPrest.vTPrest),
    valorReceber: num(vPrest.vRec),
    componentes,

    valorCarga: num(infCarga.vCarga),
    produtoPredominante: txt(infCarga.proPred),

    cst: icms.cst,
    icmsBase: icms.vBC,
    icmsAliquota: icms.pICMS,
    icmsValor: icms.vICMS,

    valorTotalTributos: num(imp.vTotTrib),

    observacoes: txt(infCte.compl?.xObs),

    documentosNFe,

    // ---- Campos do layout SSW ----
    qrCodeUrl,
    autorizacaoFl: '1/1',
    suframa: txt(infCte.dest?.ISUF),

    especie,
    valorMercadoria: num(infCarga.vCarga),
    qtdePares: qPorMedida('PARES'),
    qtdeVolumes: qPorMedida('UNIDADE', 'VOLUME'),
    cubagemM3: qPorMedida('M3', 'CUBAGEM'),
    pesoKg: qPorMedida('PESO REAL', 'PESO BRUTO'),
    pesoCalculoKg: qPorMedida('PESO BASE DE CALCULO', 'PESO BASE'),

    ibsUfPerc: num(g.gIBSUF?.pIBSUF),
    ibsUfValor: num(g.gIBSUF?.vIBSUF),
    ibsMunPerc: num(g.gIBSMun?.pIBSMun),
    ibsMunValor: num(g.gIBSMun?.vIBSMun),
    cbsPerc: num(g.gCBS?.pCBS),
    cbsValor: num(g.gCBS?.vCBS),

    situacaoTributaria: CST_ICMS[icms.cst] || (icms.cst ? `CST ${icms.cst}` : ''),
    difalIcms: 0,
    credPresIcmsSt: 0,

    tribIcmsIss: icms.vICMS,
    tribPis: 0,
    tribCofins: 0,
    tribTotal: num(imp.vTotTrib),

    prevEntrega: txt(infCte.compl?.Entrega?.comData?.dProg),
    pedido,
    rota,
    placa: '',
  };
}

/** Metadados leves só para a listagem, sem montar o CteData inteiro. */
export function parseCteResumo(xml: string): {
  numero: string;
  serie: string;
  emitenteNome: string;
  emitenteCnpj: string;
  remetenteNome: string;
  destinatarioNome: string;
  origem: string;
  destino: string;
  cfop: string;
  valor: number;
  dataEmissao: string;
} {
  const d = parseCteXml(xml);
  return {
    numero: d.numero,
    serie: d.serie,
    emitenteNome: d.emitente.nome,
    emitenteCnpj: d.emitente.cnpjCpf,
    remetenteNome: d.remetente.nome,
    destinatarioNome: d.destinatario.nome,
    origem: d.origemMunicipio && d.origemUf ? `${d.origemMunicipio}/${d.origemUf}` : '',
    destino: d.destinoMunicipio && d.destinoUf ? `${d.destinoMunicipio}/${d.destinoUf}` : '',
    cfop: d.cfop,
    valor: d.valorTotalPrestacao,
    dataEmissao: d.dataEmissao,
  };
}

function vazio(): CteData {
  const parteVazia: CtePartelE = { nome: '', cnpjCpf: '', ie: '', endereco: '', municipio: '', uf: '', cep: '', fone: '' };
  return {
    chave: '', numero: '', serie: '', modelo: '57', modal: '', cfop: '', naturezaOperacao: '',
    tipoCte: '', tipoServico: '', tomador: '', emitidoPor: '', dataEmissao: '',
    protocolo: '', dataAutorizacao: '',
    origemMunicipio: '', origemUf: '', destinoMunicipio: '', destinoUf: '',
    emitente: { ...parteVazia }, rntrc: '', remetente: { ...parteVazia }, destinatario: { ...parteVazia },
    expedidor: null, recebedor: null, tomadorParte: null,
    valorTotalPrestacao: 0, valorReceber: 0, componentes: [],
    valorCarga: 0, produtoPredominante: '',
    cst: '', icmsBase: 0, icmsAliquota: 0, icmsValor: 0,
    valorTotalTributos: 0, observacoes: '', documentosNFe: [],
    qrCodeUrl: '', autorizacaoFl: '1/1', suframa: '',
    especie: '', valorMercadoria: 0, qtdePares: 0, qtdeVolumes: 0, cubagemM3: 0, pesoKg: 0, pesoCalculoKg: 0,
    ibsUfPerc: 0, ibsUfValor: 0, ibsMunPerc: 0, ibsMunValor: 0, cbsPerc: 0, cbsValor: 0,
    situacaoTributaria: '', difalIcms: 0, credPresIcmsSt: 0,
    tribIcmsIss: 0, tribPis: 0, tribCofins: 0, tribTotal: 0,
    prevEntrega: '', pedido: '', rota: '', placa: '',
  };
}
