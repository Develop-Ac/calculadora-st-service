export type DestinacaoMercadoria = 'COMERCIALIZACAO' | 'USO_CONSUMO';
export type ImpostoEscolhido = 'ST' | 'DIFAL' | 'TRIBUTADA';

export interface FiscalConferenceItemDto {
  item: number;
  codProdFornecedor: string;
  produto?: string;
  unidadeFornecedor?: string;
  ncmNota?: string;
  cfop?: string;
  cstNota?: string;
  impostoEscolhido: ImpostoEscolhido;
  destinacaoMercadoria: DestinacaoMercadoria;
  possuiIcmsSt?: boolean;
  possuiDifal?: boolean;
  /** Código interno do produto informado manualmente pelo usuário. Quando presente, ignora a busca por vínculo no Sistema Celta e usa este código diretamente. */
  codigoInternoManual?: string;
}

export interface FiscalConferenceInvoiceDto {
  chaveNfe: string;
  itens: FiscalConferenceItemDto[];
}

export interface FiscalConferenceRequestDto {
  notas: FiscalConferenceInvoiceDto[];
}
