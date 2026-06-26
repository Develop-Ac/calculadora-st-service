/**
 * Tipos compartilhados do módulo CT-e (Conhecimento de Transporte, modelo 57).
 * Fonte única de verdade para: parser de XML (cte-xml.parser.ts), gerador do DACTE
 * (dacte/dacte.generator.ts) e respostas da API (cte.controller.ts).
 */

/** Uma parte envolvida no CT-e (emitente/remetente/destinatário/expedidor/recebedor/tomador). */
export interface CtePartelE {
  nome: string;
  cnpjCpf: string;
  ie: string;
  endereco: string; // "RUA X, 123 - BAIRRO"
  municipio: string; // "SORRISO"
  uf: string; // "MT"
  cep: string;
  fone: string;
}

/** Um componente do valor da prestação (frete) — bloco "COMPONENTES DO FRETE". */
export interface CteComponente {
  nome: string;
  valor: number;
}

/**
 * Cabeçalho/dados do CT-e já parseados do XML (infCte v4.00).
 * Tudo que a tela de Detalhe e o DACTE precisam.
 */
export interface CteData {
  // Identificação
  chave: string; // 44 dígitos
  numero: string; // nCT
  serie: string; // serie
  modelo: string; // '57'
  modal: string; // descrição do modal (ex.: "RODOVIÁRIO")
  cfop: string; // CFOP da prestação
  naturezaOperacao: string; // natOp
  tipoCte: string; // descrição de tpCTe (Normal/Complemento/Anulação/Substituto)
  tipoServico: string; // descrição de tpServ (Normal/Subcontratação/Redespacho/...)
  tomador: string; // quem é o tomador (Remetente/Expedidor/Recebedor/Destinatário/Outros)
  emitidoPor: string; // xContato do emitente (quando houver)
  dataEmissao: string; // ISO (dhEmi)

  // Autorização / protocolo
  protocolo: string; // nProt
  dataAutorizacao: string; // ISO (dhRecbto)

  // Origem / destino da prestação
  origemMunicipio: string; // xMunIni
  origemUf: string; // UFIni
  destinoMunicipio: string; // xMunFim
  destinoUf: string; // UFFim

  // Partes
  emitente: CtePartelE; // transportadora (emit) — inclui RNTRC quando houver
  rntrc: string; // RNTRC do emitente/modal rodoviário
  remetente: CtePartelE; // rem
  destinatario: CtePartelE; // dest
  expedidor: CtePartelE | null; // exped
  recebedor: CtePartelE | null; // receb

  // Valores da prestação
  valorTotalPrestacao: number; // vTPrest
  valorReceber: number; // vRec
  componentes: CteComponente[]; // Comp[]

  // Carga
  valorCarga: number; // infCarga/vCarga
  produtoPredominante: string; // proPred

  // ICMS
  cst: string; // CST do ICMS
  icmsBase: number; // vBC
  icmsAliquota: number; // pICMS
  icmsValor: number; // vICMS

  // Tributos
  valorTotalTributos: number; // vTotTrib

  // Observações
  observacoes: string; // xObs

  // Documentos transportados (chaves de NF-e referenciadas)
  documentosNFe: string[]; // infDoc/infNFe/chave[]
}

/** Linha da listagem (resumo) servida pela API e exibida na tabela da aba CT-e. */
export interface CteListItem {
  chave: string;
  numero: string;
  serie: string;
  emitente_nome: string; // transportadora
  emitente_cnpj: string;
  remetente_nome: string;
  destinatario_nome: string;
  origem: string; // "SORRISO/MT"
  destino: string; // "NOVA MUTUM/MT"
  cfop: string;
  valor: number; // valorTotalPrestacao
  data_emissao: string; // ISO
  dt_entrada: string | null; // quando LANCADA
  status: 'PENDENTE' | 'LANCADA';
}
