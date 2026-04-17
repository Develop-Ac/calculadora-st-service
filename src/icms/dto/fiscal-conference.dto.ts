export type DestinacaoMercadoria = 'COMERCIALIZACAO' | 'USO_CONSUMO';
export type ImpostoEscolhido = 'ST' | 'DIFAL' | 'TRIBUTADA';

export interface FiscalConferenceItemDto {
  item: number;
  codProdFornecedor: string;
  produto?: string;
  ncmNota?: string;
  cfop?: string;
  cstNota?: string;
  impostoEscolhido: ImpostoEscolhido;
  destinacaoMercadoria: DestinacaoMercadoria;
  possuiIcmsSt?: boolean;
  possuiDifal?: boolean;
}

export interface FiscalConferenceInvoiceDto {
  chaveNfe: string;
  itens: FiscalConferenceItemDto[];
}

export interface FiscalConferenceRequestDto {
  notas: FiscalConferenceInvoiceDto[];
}
