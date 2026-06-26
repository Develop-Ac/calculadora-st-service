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

  // RNTRC (modal rodoviário)
  const rntrc = txt(norm.infModal?.rodo?.RNTRC);

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
    emitidoPor: '',
    dataEmissao: txt(ide.dhEmi),

    protocolo: txt(proto.nProt),
    dataAutorizacao: txt(proto.dhRecbto),

    origemMunicipio: txt(ide.xMunIni),
    origemUf: txt(ide.UFIni),
    destinoMunicipio: txt(ide.xMunFim),
    destinoUf: txt(ide.UFFim),

    emitente: parseParte(emitBloco, 'enderEmit'),
    rntrc,
    remetente: parseParte(infCte.rem, 'enderReme'),
    destinatario: parseParte(infCte.dest, 'enderDest'),
    expedidor: infCte.exped ? parseParte(infCte.exped, 'enderExped') : null,
    recebedor: infCte.receb ? parseParte(infCte.receb, 'enderReceb') : null,

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
    expedidor: null, recebedor: null,
    valorTotalPrestacao: 0, valorReceber: 0, componentes: [],
    valorCarga: 0, produtoPredominante: '',
    cst: '', icmsBase: 0, icmsAliquota: 0, icmsValor: 0,
    valorTotalTributos: 0, observacoes: '', documentosNFe: [],
  };
}
