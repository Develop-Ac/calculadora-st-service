/**
 * Gerador do DACTE (Documento Auxiliar do Conhecimento de Transporte Eletrônico),
 * CT-e modelo 57, em PDF — reproduzindo FIELMENTE o layout das transportadoras
 * (padrão SSW.INF.BR).
 *
 * A geometria (grade de linhas, caixa externa, posição de cada rótulo estático e
 * de cada valor dinâmico) foi extraída pixel-a-pixel do PDF-modelo da
 * transportadora com pdfplumber e está codificada aqui em coordenadas ABSOLUTAS
 * (origem top-left, y para baixo — exatamente como o pdfkit).
 *
 * Layout: UMA página A4 contendo DUAS vias IDÊNTICAS do mesmo CT-e — uma na
 * metade superior (dY=0) e outra na metade inferior (dY≈421), separadas por uma
 * linha tracejada "Corte aqui". Cada via é desenhada por `desenharVia`, que
 * posiciona tudo em `coordenada_do_modelo + dY`.
 *
 * Código de barras Code128 (da chave de 44 dígitos) e QR Code (do qrCodeUrl) são
 * ESCANEÁVEIS, gerados com bwip-js (PNG) e embutidos como imagens nas posições
 * exatas do modelo.
 *
 * Recebe `CteData` já parseado (nenhum parsing de XML acontece aqui).
 */
import { CteData, CtePartelE, CteComponente } from '../cte.types';
import { Writable } from 'stream';

// pdfkit e bwip-js não trazem types no projeto; usamos require.
// eslint-disable-next-line @typescript-eslint/no-var-requires
const PDFDocument = require('pdfkit');
// eslint-disable-next-line @typescript-eslint/no-var-requires
const bwipjs = require('bwip-js');

// ----------------------------------------------------------------------------
// Geometria extraída do modelo (pdfplumber, coordenadas top-left absolutas)
// ----------------------------------------------------------------------------

/** Caixa externa da via (x0, top, x1, bottom). */
const VIA_BBOX: [number, number, number, number] = [14.2, 14.2, 566.9, 405.4];

/**
 * Grade COMPLETA da via (44 segmentos) no formato [x0, top, x1, bottom].
 * São as linhas internas (H e V) que dividem todos os campos do DACTE.
 */
const LINHAS_GRADE: [number, number, number, number][] = [
  [257.9, 14.2, 257.9, 320.3],
  [14.2, 62.4, 257.9, 62.4],
  [14.2, 76.5, 257.9, 76.5],
  [14.2, 90.7, 257.9, 90.7],
  [257.9, 34.0, 566.9, 34.0],
  [257.9, 113.4, 566.9, 113.4],
  [467.7, 14.2, 467.7, 34.0],
  [538.6, 14.2, 538.6, 34.0],
  [257.9, 48.2, 490.4, 48.2],
  [280.6, 34.0, 280.6, 48.2],
  [331.7, 34.0, 331.7, 48.2],
  [490.4, 34.0, 490.4, 48.2],
  [382.7, 34.0, 382.7, 48.2],
  [411.0, 34.0, 411.0, 48.2],
  [490.4, 34.0, 490.4, 48.2],
  [70.9, 62.4, 70.9, 76.5],
  [136.1, 62.4, 136.1, 76.5],
  [119.1, 76.5, 119.1, 90.7],
  [218.3, 76.5, 218.3, 90.7],
  [257.9, 121.9, 566.9, 121.9],
  [14.2, 144.6, 257.9, 144.6],
  [14.2, 204.1, 257.9, 204.1],
  [422.4, 113.4, 422.4, 231.0],
  [422.4, 178.6, 566.9, 178.6],
  [422.4, 222.5, 566.9, 222.5],
  [14.2, 231.0, 566.9, 231.0],
  [257.9, 245.2, 487.6, 245.2],
  [487.6, 231.0, 487.6, 320.3],
  [14.2, 320.3, 566.9, 320.3],
  [257.9, 195.6, 422.4, 195.6],
  [14.2, 117.6, 257.9, 117.6],
  [14.2, 171.5, 257.9, 171.5],
  [14.2, 238.1, 566.9, 238.1],
  [257.9, 252.3, 487.6, 252.3],
  [422.4, 187.1, 566.9, 187.1],
  [257.9, 222.5, 436.5, 222.5],
  [257.9, 204.1, 422.4, 204.1],
  [96.4, 320.3, 96.4, 331.6],
  [155.9, 320.3, 155.9, 331.6],
  [223.9, 320.3, 223.9, 331.6],
  [314.7, 320.3, 314.7, 331.6],
  [473.4, 379.8, 566.9, 379.8],
  [14.2, 331.6, 473.4, 331.6],
  [473.4, 320.3, 473.4, 405.3],
];

/**
 * Rótulos ESTÁTICOS (captions fixos) — posição e tamanho exatos do modelo.
 * pdfplumber reporta `top` no topo do glifo; usamos esse mesmo y (pdfkit também
 * desenha texto a partir do topo da linha).
 */
type Cap = { x: number; y: number; t: string; s: number; b?: boolean };
const CAPTIONS: Cap[] = [
  // Cabeçalho direito
  { x: 470.5, y: 15.9, t: 'AUTORIZAÇÃO', s: 5 },
  { x: 541.4, y: 15.9, t: 'FL', s: 5 },
  { x: 260.8, y: 35.8, t: 'SÉRIE', s: 5 },
  { x: 283.5, y: 35.8, t: 'NÚMERO', s: 5 },
  { x: 334.5, y: 35.8, t: 'MODAL', s: 5 },
  { x: 385.5, y: 35.8, t: 'MODELO', s: 5 },
  { x: 413.9, y: 35.8, t: 'Nº PROTOCOLO', s: 5 },
  { x: 357.3, y: 49.9, t: 'CONTROLE DO FISCO', s: 5 },
  // CNPJ/IE/RNTRC (captions na faixa do emitente)
  { x: 17.0, y: 55.6, t: 'CNPJ', s: 5 },
  { x: 90.7, y: 55.6, t: 'IE', s: 5 },
  { x: 138.9, y: 55.6, t: 'RNTRC', s: 5 },
  // Tipo CT-e / serviço / CFOP
  { x: 17.0, y: 64.1, t: 'TIPO DO CT-E', s: 5 },
  { x: 73.7, y: 64.1, t: 'TIPO DO SERVICO', s: 5 },
  { x: 138.9, y: 64.1, t: 'CFOP - NATUREZA DA PRESTAÇÃO', s: 5 },
  // Origem / destino / emitido por
  { x: 17.0, y: 78.3, t: 'ORIGEM DA PRESTAÇÃO', s: 5 },
  { x: 121.9, y: 78.3, t: 'DESTINO DA PRESTAÇÃO', s: 5 },
  { x: 221.1, y: 78.3, t: 'EMITIDO POR', s: 5 },
  // (A legenda "Chave de acesso..." e a chave formatada são renderizadas
  //  explicitamente, confinadas à área À ESQUERDA do QR — ver desenharVia.)
  // Título DACTE
  { x: 337.8, y: 16.9, t: 'D A C T E', s: 11, b: true },
  {
    x: 294.7,
    y: 27.7,
    t: 'Documento Auxiliar do Conhecimento de Transporte Eletrônico',
    s: 4.5,
  },
  // Partes (rótulos) — coluna esquerda
  { x: 17.0, y: 93.9, t: 'REMETENTE', s: 5 },
  { x: 17.0, y: 99.5, t: 'END', s: 5 },
  { x: 17.0, y: 105.2, t: 'MUN', s: 5 },
  { x: 197.0, y: 105.2, t: 'CEP', s: 5 },
  { x: 17.0, y: 110.9, t: 'CNPJ', s: 5 },
  { x: 121.9, y: 110.9, t: 'IE', s: 5 },
  { x: 192.8, y: 110.9, t: 'FONE', s: 5 },
  { x: 17.0, y: 120.8, t: 'DESTINATARIO', s: 5 },
  { x: 181.4, y: 120.8, t: 'SUFRAMA', s: 5 },
  { x: 17.0, y: 126.5, t: 'END', s: 5 },
  { x: 17.0, y: 132.1, t: 'MUN', s: 5 },
  { x: 197.0, y: 132.1, t: 'CEP', s: 5 },
  { x: 17.0, y: 137.8, t: 'CNPJ', s: 5 },
  { x: 121.9, y: 137.8, t: 'IE', s: 5 },
  { x: 192.8, y: 137.8, t: 'FONE', s: 5 },
  { x: 17.0, y: 147.7, t: 'EXPEDIDOR', s: 5 },
  { x: 17.0, y: 153.4, t: 'END', s: 5 },
  { x: 17.0, y: 159.1, t: 'MUN', s: 5 },
  { x: 197.0, y: 159.1, t: 'CEP', s: 5 },
  { x: 17.0, y: 164.7, t: 'CNPJ', s: 5 },
  { x: 121.9, y: 164.7, t: 'IE', s: 5 },
  { x: 192.8, y: 164.7, t: 'FONE', s: 5 },
  { x: 17.0, y: 174.7, t: 'RECEBEDOR/LOC ENTREGA', s: 5 },
  { x: 17.0, y: 180.3, t: 'END', s: 5 },
  { x: 17.0, y: 191.7, t: 'MUN', s: 5 },
  { x: 197.0, y: 191.7, t: 'CEP', s: 5 },
  { x: 17.0, y: 197.3, t: 'CNPJ', s: 5 },
  { x: 121.9, y: 197.3, t: 'IE', s: 5 },
  { x: 192.8, y: 197.3, t: 'FONE', s: 5 },
  { x: 17.0, y: 207.3, t: 'TOMADOR', s: 5 },
  { x: 17.0, y: 212.9, t: 'END', s: 5 },
  { x: 17.0, y: 218.6, t: 'MUN', s: 5 },
  { x: 197.0, y: 218.6, t: 'CEP', s: 5 },
  { x: 17.0, y: 224.3, t: 'CNPJ', s: 5 },
  { x: 121.9, y: 224.3, t: 'IE', s: 5 },
  { x: 192.8, y: 224.3, t: 'FONE', s: 5 },
  // Componentes do frete / mercadoria (cabeçalhos)
  { x: 300.3, y: 115.1, t: 'COMPONENTES DO FRETE (R$)', s: 5 },
  { x: 476.0, y: 115.1, t: 'MERCADORIA', s: 5 },
  // Mercadoria (rótulos)
  { x: 425.2, y: 123.6, t: 'PROD PREDOMIN', s: 5 },
  { x: 425.2, y: 129.3, t: 'ESPECIE', s: 5 },
  { x: 425.2, y: 135.0, t: 'VALOR MERCADORIA (R$)', s: 5 },
  { x: 425.2, y: 140.7, t: 'QTDE PARES/VOLUMES', s: 5 },
  { x: 425.2, y: 146.3, t: 'CUBAG(m3)/PESO (Kg)', s: 5 },
  { x: 425.2, y: 152.0, t: 'PESO CÁLCULO (Kg)', s: 5 },
  // ICMS (R$)
  { x: 481.4, y: 180.3, t: 'ICMS (R$)', s: 5 },
  { x: 425.2, y: 188.8, t: 'SITUAÇÃO TRIBUTÁRIA', s: 5 },
  { x: 425.2, y: 194.5, t: 'BASE CÁLCULO(R$)', s: 5 },
  { x: 425.2, y: 200.2, t: 'ALIQ DIFAL/ICMS(%)', s: 5 },
  { x: 425.2, y: 205.8, t: 'VALOR ICMS(R$)', s: 5 },
  { x: 425.2, y: 211.5, t: 'DIFAL ICMS ORIG/DEST(R$)', s: 5 },
  { x: 425.2, y: 217.2, t: 'CRED PRES/ICMS ST(R$)', s: 5 },
  // Reforma tributária
  { x: 314.4, y: 197.3, t: 'REFORMA TRIBUTÁRIA', s: 5 },
  { x: 260.8, y: 205.8, t: 'IBS ESTADUAL (%/R$)', s: 5 },
  { x: 260.8, y: 211.5, t: 'IBS MUNICIPAL (%/R$)', s: 5 },
  { x: 260.8, y: 217.2, t: 'CBS (%/R$)', s: 5 },
  // Frete total / valor a receber
  { x: 260.8, y: 224.3, t: 'FRETE TOTAL (R$)', s: 5 },
  { x: 425.2, y: 224.3, t: 'VALOR A RECEBER (R$)', s: 5 },
  // Observações / destaque tributos / PIX
  { x: 116.6, y: 232.8, t: 'OBSERVAÇÕES', s: 5 },
  { x: 310.7, y: 232.8, t: 'DESTAQUE DE TRIBUTOS (Lei 12.741/2012) - Em R$', s: 5 },
  { x: 523.2, y: 232.8, t: 'PIX', s: 5 },
  { x: 260.8, y: 239.9, t: 'ICMS/ISS:', s: 5 },
  { x: 323.2, y: 239.9, t: 'PIS:', s: 5 },
  { x: 368.5, y: 239.9, t: 'COFINS:', s: 5 },
  { x: 428.0, y: 239.9, t: 'TOTAL:', s: 5 },
  { x: 342.5, y: 247.0, t: 'CHAVES NF-E/CT-E/DC-E', s: 5 },
  // Faixa inferior PLACA COLETA / ...
  { x: 17.0, y: 324.9, t: 'PLACA COLETA', s: 5 },
  { x: 99.2, y: 324.9, t: 'TOMADOR', s: 5 },
  { x: 161.6, y: 324.9, t: 'COBRAR', s: 5 },
  { x: 229.6, y: 324.9, t: 'PREV.ENTREGA', s: 5 },
  { x: 317.5, y: 324.9, t: 'NR', s: 5 },
  // Declaração / tentativas
  { x: 138.2, y: 340.9, t: 'DECLARAÇÃO DE INSPEÇÃO DE ENTREGA', s: 10, b: true },
  {
    x: 19.8,
    y: 352.5,
    t: 'DECLARO PARA OS DEVIDOS FINS QUE RECEBI AS MERCADORIAS DAS COMPRAS EFETUADAS POR MIM E QUE A EMBALAGEM NÃO APRESENTAVA',
    s: 6,
  },
  {
    x: 19.8,
    y: 361.0,
    t: 'AVARIAS E NEM SINAL DE VIOLAÇÃO. DECLARO AINDA QUE AS MERCADORIAS ESTÃO DE ACORDO COM A NOTA FISCAL E EM PERFEITO ESTADO.',
    s: 6,
  },
  { x: 487.4, y: 324.9, t: 'TENTATIVAS DE ENTREGA', s: 5 },
  { x: 479.1, y: 336.3, t: '1ª ____ / ____ / ____ - ____ : ____', s: 5 },
  { x: 479.1, y: 353.3, t: '2ª ____ / ____ / ____ - ____ : ____', s: 5 },
  { x: 479.1, y: 370.3, t: '3ª ____ / ____ / ____ - ____ : ____', s: 5 },
  // Assinaturas
  { x: 19.8, y: 387.3, t: '_____________________________________', s: 5 },
  { x: 130.4, y: 387.3, t: '____________________________', s: 5 },
  { x: 218.3, y: 387.3, t: '_____________________', s: 5 },
  { x: 286.3, y: 387.3, t: '____ / ____ / ____   ____ : ____', s: 5 },
  { x: 365.7, y: 387.3, t: '____________________________________', s: 5 },
  { x: 51.0, y: 395.8, t: 'NOME LEGÍVEL', s: 5 },
  { x: 155.9, y: 395.8, t: 'RG/CPF', s: 5 },
  { x: 232.4, y: 395.8, t: 'PARENTESCO', s: 5 },
  { x: 306.1, y: 395.8, t: 'DATA/HORA', s: 5 },
  { x: 385.5, y: 395.8, t: 'ASSINATURA/CARIMBO', s: 5 },
  // Rodapé
  { x: 504.0, y: 407.9, t: 'PROCESSADO POR SSW.INF.BR', s: 4 },
];

// Posições/áreas dinâmicas (do exemplo) reaproveitadas para os valores reais ---

/** Área do Code128 (cluster de micro-barras do modelo). */
const BAR_AREA = { x: 263.6, y: 56.7, w: 486.2 - 263.6, h: 96.4 - 56.7 };
/** Área do QR Code (imagem na faixa CONTROLE DO FISCO, à direita). */
const QR_AREA = { x: 489.0, y: 34.0, w: 568.4 - 489.0, h: 113.4 - 34.0 };

// ----------------------------------------------------------------------------
// Helpers de formatação
// ----------------------------------------------------------------------------

/** Formata número como "1.234,56" (sem prefixo R$, pois os rótulos já trazem R$). */
function brl(valor: number | null | undefined): string {
  if (valor === null || valor === undefined || isNaN(valor)) return '0,00';
  return valor.toLocaleString('pt-BR', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

/** Número simples pt-BR com N casas. */
function num(valor: number | null | undefined, casas = 2): string {
  if (valor === null || valor === undefined || isNaN(valor)) {
    return casas > 0 ? '0,' + '0'.repeat(casas) : '0';
  }
  return valor.toLocaleString('pt-BR', {
    minimumFractionDigits: casas,
    maximumFractionDigits: casas,
  });
}

/** Texto seguro com fallback vazio (não "-"). */
function txtv(v: string | number | null | undefined): string {
  if (v === null || v === undefined) return '';
  return String(v).trim();
}

/** Converte ISO -> "dd/MM/yy HH:mm". */
function dataHora(iso: string | null | undefined): string {
  if (!iso) return '';
  const d = new Date(iso);
  if (isNaN(d.getTime())) return '';
  const dd = String(d.getDate()).padStart(2, '0');
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const yy = String(d.getFullYear()).slice(-2);
  const hh = String(d.getHours()).padStart(2, '0');
  const mi = String(d.getMinutes()).padStart(2, '0');
  return `${dd}/${mm}/${yy} ${hh}:${mi}`;
}

/** Converte ISO -> "dd/MM/yy". */
function dataCurta(iso: string | null | undefined): string {
  if (!iso) return '';
  const d = new Date(iso);
  if (isNaN(d.getTime())) return '';
  const dd = String(d.getDate()).padStart(2, '0');
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const yy = String(d.getFullYear()).slice(-2);
  return `${dd}/${mm}/${yy}`;
}

/** Formata um CNPJ (14 díg) ou CPF (11 díg). */
function cnpjFmt(v: string | null | undefined): string {
  const c = (v || '').replace(/\D/g, '');
  if (c.length === 14) {
    return c.replace(/(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, '$1.$2.$3/$4-$5');
  }
  if (c.length === 11) {
    return c.replace(/(\d{3})(\d{3})(\d{3})(\d{2})/, '$1.$2.$3-$4');
  }
  return txtv(v);
}

/** Formata CEP (8 díg) -> "00000-000". */
function cepFmt(v: string | null | undefined): string {
  const c = (v || '').replace(/\D/g, '');
  if (c.length === 8) return c.replace(/(\d{5})(\d{3})/, '$1-$2');
  return txtv(v);
}

/** Zero-pad numérico. */
function pad(v: string | number | null | undefined, n: number): string {
  const s = String(v ?? '').replace(/\D/g, '');
  if (!s) return '';
  return s.padStart(n, '0');
}

/**
 * Formata a chave de acesso (44 díg) no padrão SSW por blocos com hífens:
 * UF.AAMM.CNPJ-mod-serie-nCT(000.000.000)-tpEmis-cCT(00.000.000)-cDV
 */
function chaveFormatada(chave: string | null | undefined): string {
  const c = (chave || '').replace(/\D/g, '');
  if (c.length !== 44) return formatarChaveBlocos(c);
  const uf = c.slice(0, 2);
  const aamm = c.slice(2, 6);
  const cnpj = c.slice(6, 20);
  const mod = c.slice(20, 22);
  const serie = c.slice(22, 25);
  const nCT = c.slice(25, 34);
  const tpEmis = c.slice(34, 35);
  const cCT = c.slice(35, 43);
  const cDV = c.slice(43, 44);
  const cnpjF = cnpj.replace(/(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, '$1.$2.$3/$4-$5');
  const nCTf = nCT.replace(/(\d{3})(\d{3})(\d{3})/, '$1.$2.$3');
  const cCTf = cCT.replace(/(\d{2})(\d{3})(\d{3})/, '$1.$2.$3');
  return `${uf}.${aamm}.${cnpjF}-${mod}-${serie}-${nCTf}-${tpEmis}-${cCTf}-${cDV}`;
}

/** Fallback: agrupa em blocos de 4 se não tiver 44 dígitos. */
function formatarChaveBlocos(c: string): string {
  if (!c) return '';
  return c.replace(/(.{4})/g, '$1 ').trim();
}

// ----------------------------------------------------------------------------
// Gerador
// ----------------------------------------------------------------------------

export async function gerarDacte(data: CteData): Promise<Buffer> {
  // Gera UMA vez os códigos escaneáveis (PNG) e reusa nas duas vias.
  const chaveDigits = (data.chave || '').replace(/\D/g, '');
  let barBuf: Buffer | null = null;
  let qrBuf: Buffer | null = null;

  if (chaveDigits) {
    try {
      barBuf = await bwipjs.toBuffer({
        bcid: 'code128',
        text: chaveDigits,
        scale: 3,
        height: 8,
        includetext: false,
        paddingwidth: 0,
        paddingheight: 0,
      });
    } catch {
      barBuf = null;
    }
  }
  try {
    qrBuf = await bwipjs.toBuffer({
      bcid: 'qrcode',
      text: data.qrCodeUrl || data.chave || ' ',
      scale: 3,
    });
  } catch {
    qrBuf = null;
  }

  return new Promise<Buffer>((resolve, reject) => {
    try {
      const doc = new PDFDocument({
        size: 'A4',
        margin: 0,
        bufferPages: true,
        autoFirstPage: true,
      });

      // Blinda contra qualquer quebra automática de página: todo o conteúdo é
      // posicionado por coordenadas absolutas. Neutralizamos addPage.
      doc.addPage = function () {
        return doc;
      };

      const chunks: Buffer[] = [];
      const stream = new Writable({
        write(chunk: Buffer, _encoding: string, callback: () => void) {
          chunks.push(Buffer.from(chunk));
          callback();
        },
      });

      stream.on('finish', () => resolve(Buffer.concat(chunks)));
      stream.on('error', reject);
      doc.on('error', reject);

      doc.pipe(stream);

      const pageHeight = doc.page.height; // ~842
      const metade = pageHeight / 2; // ~421

      desenharVia(doc, data, 0, barBuf, qrBuf);
      desenharVia(doc, data, metade, barBuf, qrBuf);

      // Linha tracejada de corte no meio da folha, com texto "Corte aqui".
      doc
        .save()
        .lineWidth(0.5)
        .dash(3, { space: 2 })
        .strokeColor('#666666')
        .moveTo(VIA_BBOX[0], metade)
        .lineTo(VIA_BBOX[2], metade)
        .stroke()
        .undash()
        .restore();
      doc
        .fontSize(4)
        .font('Helvetica')
        .fillColor('#666666')
        .text('Corte aqui', VIA_BBOX[0], metade - 5, {
          width: VIA_BBOX[2] - VIA_BBOX[0],
          align: 'left',
          lineBreak: false,
        });
      doc.fillColor('#000000');

      doc.end();
    } catch (err) {
      reject(err);
    }
  });
}

// ----------------------------------------------------------------------------
// Desenho de uma via (metade da folha), por coordenada ABSOLUTA + dY
// ----------------------------------------------------------------------------

function desenharVia(
  doc: any,
  data: CteData,
  dY: number,
  barBuf: Buffer | null,
  qrBuf: Buffer | null,
): void {
  doc.strokeColor('#000000').fillColor('#000000').lineWidth(0.5);

  // --- Helpers locais (coordenadas do modelo, deslocadas por dY) -------------

  /**
   * Ajusta um texto livre para caber em UMA linha dentro de `maxWidth`:
   *   1) tenta encolher a fonte em até ~1pt (passos de 0.5, piso 4pt);
   *   2) se ainda não couber, trunca por caractere e acrescenta "…".
   * Mede com `doc.widthOfString` no font correto. Retorna texto+tamanho finais.
   * Garante que o valor JAMAIS cruze as linhas da grade.
   */
  const fit = (
    str: string,
    maxWidth: number,
    size: number,
    bold: boolean,
    floorOverride?: number,
  ): { text: string; size: number } => {
    const fontName = bold ? 'Helvetica-Bold' : 'Helvetica';
    const fits = (t: string, s: number) => {
      doc.font(fontName).fontSize(s);
      return doc.widthOfString(t) <= maxWidth;
    };
    if (!str) return { text: '', size };
    if (fits(str, size)) return { text: str, size };
    // 1) reduzir a fonte até o piso (padrão: size-1, min 4pt; ou floorOverride
    //    para textos que NÃO devem ser truncados, ex.: a chave de acesso).
    const floor = floorOverride !== undefined ? floorOverride : Math.max(4, size - 1);
    let s = size;
    while (s - 0.5 >= floor) {
      s = Math.round((s - 0.5) * 10) / 10;
      if (fits(str, s)) return { text: str, size: s };
    }
    // 2) truncar por caractere com reticências, no menor tamanho testado
    let t = str;
    while (t.length > 1 && !fits(t + '…', s)) {
      t = t.slice(0, -1).trimEnd();
    }
    return { text: t.length > 1 ? t + '…' : t, size: s };
  };

  /** Texto verbatim na posição/tamanho do modelo, sempre 1 linha que cabe na célula. */
  const put = (
    x: number,
    y: number,
    str: string,
    size: number,
    opts: { bold?: boolean; w?: number; align?: string; floor?: number } = {},
  ) => {
    if (str === undefined || str === null || str === '') return;
    const w = opts.w ?? 400;
    const f = fit(String(str), w - 1, size, !!opts.bold, opts.floor);
    doc
      .fontSize(f.size)
      .font(opts.bold ? 'Helvetica-Bold' : 'Helvetica')
      .fillColor('#000000')
      .text(f.text, x, y + dY, {
        width: w,
        align: (opts.align as any) ?? 'left',
        ellipsis: false,
        lineBreak: false,
      });
  };

  /**
   * Texto alinhado à direita, ancorado em xRight (borda direita da célula).
   * `pad` deixa ~3pt antes da borda; `xLeftLimit` (opcional) é o x_fim_do_rótulo
   * da mesma linha: a largura útil vira (xRight − pad − xLeftLimit), garantindo
   * um gap mínimo entre rótulo e valor (o valor encolhe/trunca para caber).
   */
  const putR = (
    xRight: number,
    y: number,
    str: string,
    size: number,
    w = 60,
    bold = true,
    xLeftLimit?: number,
    pad = 3,
  ) => {
    if (str === undefined || str === null || str === '') return;
    const right = xRight - pad;
    // largura disponível: limitada por w e, se houver rótulo à esquerda, pelo gap.
    let avail = w - pad;
    if (xLeftLimit !== undefined) avail = Math.min(avail, right - xLeftLimit);
    if (avail < 4) avail = 4;
    const f = fit(String(str), avail, size, bold);
    doc
      .fontSize(f.size)
      .font(bold ? 'Helvetica-Bold' : 'Helvetica')
      .fillColor('#000000')
      .text(f.text, right - avail, y + dY, {
        width: avail,
        align: 'right',
        ellipsis: false,
        lineBreak: false,
      });
  };

  // ===========================================================================
  // 1) GRADE EXATA: caixa externa + todas as linhas internas do blueprint
  // ===========================================================================
  doc.lineWidth(0.8);
  doc
    .rect(VIA_BBOX[0], VIA_BBOX[1] + dY, VIA_BBOX[2] - VIA_BBOX[0], VIA_BBOX[3] - VIA_BBOX[1])
    .stroke();
  doc.lineWidth(0.5);
  for (const [x0, t0, x1, b1] of LINHAS_GRADE) {
    doc
      .moveTo(x0, t0 + dY)
      .lineTo(x1, b1 + dY)
      .stroke();
  }

  // ===========================================================================
  // 2) RÓTULOS ESTÁTICOS (verbatim, posição/tamanho do modelo)
  // ===========================================================================
  for (const c of CAPTIONS) {
    put(c.x, c.y, c.t, c.s, { bold: !!c.b, w: 320 });
  }

  // ===========================================================================
  // 4) BARCODE (Code128) e QR nas posições EXATAS do modelo
  // ===========================================================================
  if (barBuf) {
    doc.image(barBuf, BAR_AREA.x, BAR_AREA.y + dY, { width: BAR_AREA.w, height: BAR_AREA.h });
  }
  if (qrBuf) {
    // QR no quadrado da faixa CONTROLE DO FISCO (à direita). Mantemos quadrado.
    const qs = Math.min(QR_AREA.w, QR_AREA.h);
    doc.image(qrBuf, QR_AREA.x, QR_AREA.y + dY, { width: qs, height: qs });
  }

  // ===========================================================================
  // 3) VALORES DINÂMICOS (CteData) nas posições do exemplo
  // ===========================================================================
  const emit = data.emitente || ({} as CtePartelE);

  // --- Emitente (topo-esquerda, x≈121.9; logo ocuparia x 17→116) -------------
  const emX = 121.9;
  const emW = 257.9 - emX - 2;
  put(emX, 18.0, txtv(emit.nome), 6, { bold: true, w: emW });
  let ey = 23.7;
  if (txtv(emit.endereco)) {
    put(emX, ey, txtv(emit.endereco), 6, { w: emW });
    ey += 5.6;
  }
  const emMun = [txtv(emit.municipio), txtv(emit.uf)].filter(Boolean).join(' -');
  const emLinha = `${emMun}${txtv(emit.cep) ? '  -CEP: ' + cepFmt(emit.cep) : ''}`;
  if (emLinha.trim()) {
    put(emX, ey, emLinha, 6, { w: emW });
    ey += 5.6;
  }
  if (txtv(emit.fone)) {
    put(emX, ey, 'FONE: ' + txtv(emit.fone), 6, { w: emW });
  }

  // --- AUTORIZAÇÃO (data/hora) | FL ------------------------------------------
  put(470.5, 22.3, dataHora(data.dataAutorizacao), 7, { bold: true, w: 68 });
  put(541.4, 22.3, txtv(data.autorizacaoFl), 7, { bold: true, w: 25 });

  // --- SÉRIE / NÚMERO / MODAL / MODELO / PROTOCOLO ---------------------------
  // Célula y34→48.2: rótulos no topo (cap em y35.8); valores na metade INFERIOR
  // (baseline y≈41), fonte 6.5pt bold, com folga do rótulo e sem tocar y48.2.
  const snY = 41.0; // baseline dos valores na parte de baixo da célula
  put(260.8, snY, txtv(data.serie), 6.5, { bold: true, w: 19 });
  put(283.5, snY, pad(data.numero, 9), 6.5, { bold: true, w: 48 });
  put(334.5, snY, txtv(data.modal), 6.5, { bold: true, w: 48 });
  put(385.5, snY, txtv(data.modelo) || '57', 6.5, { bold: true, w: 25 });
  put(413.9, snY, txtv(data.protocolo), 6.5, { bold: true, w: 76 });

  // --- CNPJ / IE / RNTRC do emitente (valores) -------------------------------
  put(34.0, 54.8, cnpjFmt(emit.cnpjCpf), 6, { bold: false, w: 56 });
  put(96.4, 54.8, txtv(emit.ie), 6, { bold: false, w: 40 });
  put(158.7, 54.8, txtv(data.rntrc), 6, { bold: false, w: 96 });

  // --- TIPO CT-E / TIPO SERVIÇO / CFOP - NATUREZA ----------------------------
  put(17.0, 69.0, txtv(data.tipoCte), 6, { bold: true, w: 54 });
  put(73.7, 69.0, txtv(data.tipoServico), 6, { bold: true, w: 62 });
  put(138.9, 69.0, `${txtv(data.cfop)} ${txtv(data.naturezaOperacao)}`.trim(), 6, {
    bold: false,
    w: 117,
  });

  // --- ORIGEM / DESTINO / EMITIDO POR ----------------------------------------
  put(17.0, 83.2, `${txtv(data.origemMunicipio)}/${txtv(data.origemUf)}`, 6, {
    bold: false,
    w: 100,
  });
  put(121.9, 83.2, `${txtv(data.destinoMunicipio)}/${txtv(data.destinoUf)}`, 6, {
    bold: false,
    w: 96,
  });
  put(221.1, 83.2, txtv(data.emitidoPor), 6, { bold: false, w: 35 });

  // --- Legenda + Chave formatada (confinadas À ESQUERDA do QR) ---------------
  // O QR começa em QR_AREA.x (~489); legenda e chave ocupam só x 258 → ~483,
  // centralizadas nesse intervalo, para nunca encostar/entrar sob o QR.
  {
    const chvX = 258.0;
    const chvRight = QR_AREA.x - 6; // borda direita útil (esquerda do QR)
    const chvW = chvRight - chvX; // ~225pt
    // Legenda estática (centralizada no intervalo esquerdo).
    doc
      .fontSize(5)
      .font('Helvetica')
      .fillColor('#000000')
      .text(
        'Chave de acesso para consulta de autenticidade no site www.cte.fazenda.gov.br',
        chvX,
        99.5 + dY,
        { width: chvW, align: 'center', lineBreak: false, ellipsis: false },
      );
    // Chave formatada (negrito), encolhe a fonte se preciso para caber em 1 linha
    // dentro do intervalo esquerdo (fit já mede com widthOfString).
    put(chvX, 105.1, chaveFormatada(data.chave), 7, {
      bold: true,
      w: chvW,
      align: 'center',
      floor: 4, // encolhe até caber numa linha; nunca trunca a chave
    });
  }

  // ===========================================================================
  // PARTES (coluna esquerda) — preenche cada bloco pelos rótulos do modelo
  // ===========================================================================
  // Cada parte tem o nome na linha do rótulo (à direita do caption), e END/MUN/
  // CEP/CNPJ/IE/FONE nas linhas seguintes. Coordenadas do modelo:
  //   REMETENTE:    nome y93.1; END 98.8; MUN 104.4; CEP 209.8; CNPJ 110.1; ...
  //   DESTINATARIO: nome y120.0; END 125.7; MUN 131.4; ...
  //   EXPEDIDOR:    nome y147.0; END 152.6; MUN 158.3; ...
  //   RECEBEDOR:    nome y173.9; END 179.6; (SETOR/MUN 185.2/190.9); ...
  //   TOMADOR:      nome y206.5; END 212.2; MUN 217.8; ...
  const parteW = 257.9 - 34.0 - 2; // valores começam após os captions (x≈34)
  const drawParte = (
    p: CtePartelE | null | undefined,
    yNome: number,
    yEnd: number,
    yMun: number,
    yCnpj: number,
    nomeX = 51.0,
    suframa?: string,
  ) => {
    if (!p) return;
    put(nomeX, yNome, txtv(p.nome), 6, { bold: true, w: 257.9 - nomeX - 2 });
    if (suframa !== undefined && txtv(suframa)) {
      put(210.0, yNome, txtv(suframa), 5.5, { bold: false, w: 46 });
    }
    put(34.0, yEnd, txtv(p.endereco), 6, { bold: false, w: parteW });
    const mun = `${txtv(p.municipio)}${txtv(p.uf) ? ' - ' + txtv(p.uf) : ''}`;
    put(34.0, yMun, mun, 6, { bold: false, w: 160 });
    put(209.8, yMun, cepFmt(p.cep), 6, { bold: false, w: 47 });
    put(34.0, yCnpj, cnpjFmt(p.cnpjCpf), 6, { bold: false, w: 95 });
    put(133.2, yCnpj, txtv(p.ie), 6, { bold: false, w: 56 });
    put(209.8, yCnpj, txtv(p.fone), 6, { bold: false, w: 47 });
  };

  // Resolve a parte do TOMADOR
  const tomadorParte = (): CtePartelE | null => {
    const t = (data.tomador || '').toUpperCase();
    if (t.includes('DESTINAT')) return data.destinatario;
    if (t.includes('EXPEDIDOR')) return data.expedidor || data.remetente;
    if (t.includes('RECEBEDOR')) return data.recebedor || data.remetente;
    if (t.includes('REMET')) return data.remetente;
    return data.remetente;
  };

  drawParte(data.remetente, 93.1, 98.8, 104.4, 110.1);
  drawParte(data.destinatario, 120.0, 125.7, 131.4, 137.0, 51.0, txtv(data.suframa));
  drawParte(data.expedidor, 147.0, 152.6, 158.3, 164.0);
  // RECEBEDOR tem o rótulo mais largo ("RECEBEDOR/LOC ENTREGA"), então o nome
  // começa mais à direita (x≈111) para não encostar no rótulo.
  drawParte(data.recebedor, 173.9, 179.6, 190.9, 196.6, 111.0);
  drawParte(tomadorParte(), 206.5, 212.2, 217.8, 223.5);

  // ===========================================================================
  // OBSERVAÇÕES (banda y 238.1 → 320.3, coluna esquerda x 14→257.9)
  // ===========================================================================
  {
    const obsX = 17.0;
    const obsTop = 239.0;
    const obsW = 257.9 - obsX - 3;
    const obsFonte = 6;
    const charsPorLinha = Math.max(1, Math.floor(obsW / (obsFonte * 0.5)));
    const maxLinhas = 13;
    const obsTxt = txtv(data.observacoes);
    if (obsTxt) {
      // Quebra manual por orçamento de caracteres (sem invadir faixas vizinhas).
      const palavras = obsTxt.split(/\s+/);
      const linhas: string[] = [];
      let atual = '';
      for (const w of palavras) {
        if ((atual + ' ' + w).trim().length > charsPorLinha) {
          if (atual) linhas.push(atual);
          atual = w;
        } else {
          atual = (atual ? atual + ' ' : '') + w;
        }
        if (linhas.length >= maxLinhas) break;
      }
      if (atual && linhas.length < maxLinhas) linhas.push(atual);
      doc.fontSize(obsFonte).font('Helvetica').fillColor('#000000');
      linhas.slice(0, maxLinhas).forEach((ln, i) => {
        doc.text(ln, obsX, obsTop + dY + i * 5.6, { width: obsW, lineBreak: false });
      });
    }
  }

  // ===========================================================================
  // COMPONENTES DO FRETE (sub-coluna x 257.9 → 422.4, y 121.5 →)
  // ===========================================================================
  {
    const comps: CteComponente[] = Array.isArray(data.componentes) ? data.componentes : [];
    const cX = 260.8;
    const cValRight = 420.0; // valores alinhados à direita da sub-coluna
    let cy = 121.5;
    const maxComps = 17;
    comps.slice(0, maxComps).forEach((c) => {
      put(cX, cy, txtv(c.nome), 6, { bold: false, w: 95 });
      putR(cValRight, cy, brl(c.valor), 6, 45, true, 358.0); // nome até ~x356
      cy += 5.7;
    });
  }

  // ===========================================================================
  // MERCADORIA (sub-coluna x 422.4 → 566.9). Valores à direita dos rótulos.
  // ===========================================================================
  {
    const mRight = 565.0;
    // PROD PREDOMIN / ESPECIE: valor logo após o rótulo, na mesma faixa.
    put(496.0, 121.5, txtv(data.produtoPredominante), 6, { bold: false, w: 67 });
    put(496.0, 127.2, txtv(data.especie), 6, { bold: false, w: 67 });
    // Números à direita: gap a partir do fim do rótulo (~x490) + 3pt de padding.
    const mLbl = 490.0;
    putR(mRight, 132.9, brl(data.valorMercadoria), 6, 60, false, mLbl);
    putR(mRight, 138.5, `${num(data.qtdePares, 0)} / ${num(data.qtdeVolumes, 0)}`, 6, 80, false, mLbl);
    putR(mRight, 144.2, `${num(data.cubagemM3, 4)} / ${num(data.pesoKg, 3)}`, 6, 90, false, mLbl);
    putR(mRight, 149.9, num(data.pesoCalculoKg, 3), 6, 60, false, mLbl);
  }

  // ===========================================================================
  // ICMS (R$) — valores à direita; algumas linhas têm DOIS valores (col 497/545)
  // ===========================================================================
  {
    const c1 = 510.0; // borda direita da 1ª coluna de valor
    const c2 = 565.0; // borda direita da 2ª coluna de valor
    const iLbl = 490.0; // fim aproximado dos rótulos ICMS (x425→~490)
    putR(c2, 186.7, txtv(data.situacaoTributaria), 6, 74, false, iLbl); // SITUAÇÃO TRIBUTÁRIA
    putR(c2, 192.4, brl(data.icmsBase), 6, 30, false, iLbl); // BASE CÁLCULO
    putR(c1, 198.1, num(data.icmsAliquota), 6, 18, false, iLbl); // ALIQ DIFAL
    putR(c2, 198.1, num(data.icmsAliquota), 6, 18, false); // ALIQ ICMS
    putR(c2, 203.7, brl(data.icmsValor), 6, 30, false, iLbl); // VALOR ICMS
    putR(c1, 209.4, brl(data.difalIcms), 6, 18, false, iLbl); // DIFAL ORIG
    putR(c2, 209.4, brl(data.difalIcms), 6, 18, false); // DIFAL DEST
    putR(c1, 215.1, brl(data.credPresIcmsSt), 6, 18, false, iLbl); // CRED PRES
    putR(c2, 215.1, brl(data.credPresIcmsSt), 6, 18, false); // ICMS ST
  }

  // ===========================================================================
  // REFORMA TRIBUTÁRIA (x 257.9 → 422.4): % (col ~344) e R$ (col ~401)
  // ===========================================================================
  {
    const pRight = 360.0; // borda direita da coluna de %
    const vRight = 420.0; // borda direita da coluna de R$
    const rLbl = 322.0; // fim aproximado dos rótulos IBS/CBS (x260→~320)
    putR(pRight, 203.7, num(data.ibsUfPerc), 6, 24, false, rLbl);
    putR(vRight, 203.7, brl(data.ibsUfValor), 6, 30, false);
    putR(pRight, 209.4, num(data.ibsMunPerc), 6, 24, false, rLbl);
    putR(vRight, 209.4, brl(data.ibsMunValor), 6, 30, false);
    putR(pRight, 215.1, num(data.cbsPerc), 6, 24, false, rLbl);
    putR(vRight, 215.1, brl(data.cbsValor), 6, 30, false);
  }

  // ===========================================================================
  // FRETE TOTAL (x 257.9→436.5) | VALOR A RECEBER (x 422.4→566.9)
  // ===========================================================================
  putR(420.0, 223.6, brl(data.valorTotalPrestacao), 6, 60, false, 309.0); // rótulo FRETE TOTAL ~x308
  putR(565.0, 223.6, brl(data.valorReceber), 6, 60, false, 499.0); // rótulo VALOR A RECEBER ~x498

  // ===========================================================================
  // DESTAQUE DE TRIBUTOS (Lei 12.741) — valores após cada rótulo (y 237.7)
  // ===========================================================================
  put(296.0, 237.7, brl(data.tribIcmsIss), 7, { bold: false, w: 26 });
  put(345.0, 237.7, brl(data.tribPis), 7, { bold: false, w: 22 });
  put(401.0, 237.7, brl(data.tribCofins), 7, { bold: false, w: 26 });
  put(460.0, 237.7, brl(data.tribTotal), 7, { bold: false, w: 26 });

  // ===========================================================================
  // CHAVES NF-E/CT-E/DC-E (x 257.9 → 487.6, y 253.4 →)
  // ===========================================================================
  {
    const nfes: string[] = Array.isArray(data.documentosNFe) ? data.documentosNFe : [];
    let ny = 253.4;
    const nMax = 8;
    nfes.slice(0, nMax).forEach((ch) => {
      put(260.8, ny, 'NF-E: ' + (ch || '').replace(/\D/g, ''), 7, {
        bold: false,
        w: 487.6 - 260.8 - 3,
      });
      ny += 5.7;
    });
  }

  // ===========================================================================
  // Faixa inferior: valores PLACA / TOMADOR / COBRAR / PREV.ENTREGA / NR
  // (captions em y 324.9; valores do exemplo em y 322.8)
  // ===========================================================================
  put(59.5, 322.8, txtv(data.placa), 7, { bold: true, w: 36 });
  put(127.6, 322.8, txtv(data.tomador), 7, { bold: true, w: 28 });
  put(272.1, 322.8, dataCurta(data.prevEntrega), 7, { bold: true, w: 42 });
  put(328.8, 322.8, txtv(data.pedido), 7, { bold: true, w: 144 });

  // ===========================================================================
  // ROTA / placa-bloco grande no canto inferior-direito (SRS / CLD/785 no modelo)
  // — usamos a rota do CT-e quando houver.
  // ===========================================================================
  if (txtv(data.rota)) {
    put(476.0, 382.1, txtv(data.rota), 8, { bold: true, w: 90, align: 'center' });
  }
}
