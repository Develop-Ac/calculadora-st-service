/**
 * Gerador do DACTE (Documento Auxiliar do Conhecimento de Transporte Eletrônico),
 * CT-e modelo 57, em PDF usando pdfkit.
 *
 * Layout: página A4 contendo DUAS vias idênticas do mesmo CT-e — uma na metade
 * superior e outra na metade inferior da folha.
 *
 * Recebe `CteData` já parseado (nenhum parsing de XML acontece aqui).
 */
import { CteData, CtePartelE, CteComponente } from '../cte.types';
import { Writable } from 'stream';

// pdfkit não traz types (nem há @types/pdfkit no projeto); usamos require para
// evitar erros de compilação no tsconfig do NestJS.
// eslint-disable-next-line @typescript-eslint/no-var-requires
const PDFDocument = require('pdfkit');

// ----------------------------------------------------------------------------
// Helpers de formatação
// ----------------------------------------------------------------------------

/** Formata número como moeda pt-BR: 1234.56 -> "R$ 1.234,56". */
function brl(valor: number | null | undefined): string {
  if (valor === null || valor === undefined || isNaN(valor)) return '-';
  return (
    'R$ ' +
    valor.toLocaleString('pt-BR', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })
  );
}

/** Número simples pt-BR (sem prefixo R$). */
function num(valor: number | null | undefined, casas = 2): string {
  if (valor === null || valor === undefined || isNaN(valor)) return '-';
  return valor.toLocaleString('pt-BR', {
    minimumFractionDigits: casas,
    maximumFractionDigits: casas,
  });
}

/** Texto seguro: null/undefined/vazio -> "-". */
function txt(v: string | null | undefined): string {
  if (v === null || v === undefined) return '-';
  const t = String(v).trim();
  return t.length ? t : '-';
}

/** Converte ISO -> "dd/MM/yyyy HH:mm". Tolera valor vazio/ inválido. */
function dataHora(iso: string | null | undefined): string {
  if (!iso) return '-';
  const d = new Date(iso);
  if (isNaN(d.getTime())) return '-';
  const dd = String(d.getDate()).padStart(2, '0');
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const yyyy = d.getFullYear();
  const hh = String(d.getHours()).padStart(2, '0');
  const mi = String(d.getMinutes()).padStart(2, '0');
  return `${dd}/${mm}/${yyyy} ${hh}:${mi}`;
}

/** Agrupa a chave de 44 dígitos em blocos de 4 para leitura. */
function formatarChave(chave: string | null | undefined): string {
  const c = (chave || '').replace(/\D/g, '');
  if (!c) return '-';
  return c.replace(/(.{4})/g, '$1 ').trim();
}

/** Monta a linha de endereço completa de uma parte. */
function enderecoCompleto(p: CtePartelE): string {
  const partes: string[] = [];
  if (p.endereco && p.endereco.trim()) partes.push(p.endereco.trim());
  const munUf: string[] = [];
  if (p.municipio && p.municipio.trim()) munUf.push(p.municipio.trim());
  if (p.uf && p.uf.trim()) munUf.push(p.uf.trim());
  if (munUf.length) partes.push(munUf.join('/'));
  if (p.cep && p.cep.trim()) partes.push('CEP ' + p.cep.trim());
  return partes.length ? partes.join(' - ') : '-';
}

// ----------------------------------------------------------------------------
// Gerador
// ----------------------------------------------------------------------------

export async function gerarDacte(data: CteData): Promise<Buffer> {
  return new Promise<Buffer>((resolve, reject) => {
    try {
      const doc = new PDFDocument({
        size: 'A4',
        margin: 18,
        bufferPages: true,
        autoFirstPage: true,
      });

      // Blinda contra qualquer quebra automática de página: como todo o conteúdo
      // é posicionado por coordenadas absolutas dentro de cada via, uma 2ª página
      // só apareceria por overflow de texto. Neutralizamos addPage.
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

      // Página A4 ~ 595 x 842 pt. Cada via ocupa metade vertical.
      const pageHeight = doc.page.height; // ~842
      const metade = pageHeight / 2; // ~421

      const dataImpressao = dataHora(new Date().toISOString());

      // Desenha a via no topo (offsetY=0) e a via inferior (offsetY=metade).
      desenharVia(doc, data, 0, dataImpressao);
      desenharVia(doc, data, metade, dataImpressao);

      // Linha pontilhada de corte no meio da folha.
      const left = doc.page.margins.left;
      const right = doc.page.width - doc.page.margins.right;
      doc
        .save()
        .lineWidth(0.5)
        .dash(3, { space: 2 })
        .strokeColor('#999999')
        .moveTo(left, metade)
        .lineTo(right, metade)
        .stroke()
        .undash()
        .restore();

      doc.end();
    } catch (err) {
      reject(err);
    }
  });
}

// ----------------------------------------------------------------------------
// Desenho de uma via (metade da folha)
// ----------------------------------------------------------------------------

function desenharVia(
  doc: any,
  data: CteData,
  offsetY: number,
  dataImpressao: string,
): void {
  const left = doc.page.margins.left; // 20
  const right = doc.page.width - doc.page.margins.right; // ~575
  const width = right - left; // ~555

  // y inicial da via. Topo da página tem margem de 18pt; nas vias começamos
  // logo abaixo do offset para garantir que a via inteira caiba em ~395pt.
  let y = offsetY + 4;

  doc.strokeColor('#000000').fillColor('#000000').lineWidth(0.6);

  // Helpers locais de desenho ------------------------------------------------

  const box = (x: number, by: number, w: number, h: number) => {
    doc.rect(x, by, w, h).stroke();
  };

  /** Rótulo pequeno em cima + valor abaixo, dentro de uma célula. */
  const cell = (
    x: number,
    by: number,
    w: number,
    h: number,
    label: string,
    value: string,
    opts: { valueSize?: number; labelSize?: number; align?: string } = {},
  ) => {
    box(x, by, w, h);
    const labelSize = opts.labelSize ?? 5;
    const valueSize = opts.valueSize ?? 7;
    const align = (opts.align ?? 'left') as any;
    doc
      .fontSize(labelSize)
      .font('Helvetica')
      .fillColor('#000000')
      .text(label, x + 2, by + 1.5, { width: w - 4, align });
    doc
      .fontSize(valueSize)
      .font('Helvetica-Bold')
      .text(value, x + 2, by + labelSize + 2.5, {
        width: w - 4,
        align,
        height: h - labelSize - 4,
        ellipsis: true,
      });
    doc.font('Helvetica');
  };

  /** Texto livre dentro de uma caixa, sem moldura própria. */
  const textIn = (
    x: number,
    by: number,
    w: number,
    str: string,
    size = 6,
    bold = false,
  ) => {
    doc
      .fontSize(size)
      .font(bold ? 'Helvetica-Bold' : 'Helvetica')
      .fillColor('#000000')
      .text(str, x + 2, by, { width: w - 4, ellipsis: true });
    doc.font('Helvetica');
  };

  // ===========================================================================
  // 1) CABEÇALHO: emitente (esq) | título + impressão + grade (dir)
  // ===========================================================================
  const headerH = 64;
  const emitW = width * 0.42;
  const dirX = left + emitW;
  const dirW = width - emitW;

  // Caixa emitente
  box(left, y, emitW, headerH);
  const emit = data.emitente;
  doc
    .fontSize(8)
    .font('Helvetica-Bold')
    .fillColor('#000000')
    .text(txt(emit?.nome), left + 3, y + 4, { width: emitW - 6, ellipsis: true });
  doc
    .fontSize(6)
    .font('Helvetica')
    .text(txt(emit?.endereco), left + 3, y + 16, {
      width: emitW - 6,
      ellipsis: true,
    });
  const emitMun = [emit?.municipio, emit?.uf].filter(Boolean).join('/');
  doc.text(
    `${emitMun || '-'}${emit?.cep ? '  CEP ' + emit.cep : ''}`,
    left + 3,
    y + 26,
    { width: emitW - 6, ellipsis: true },
  );
  if (emit?.fone) {
    doc.text('FONE ' + txt(emit.fone), left + 3, y + 36, {
      width: emitW - 6,
      ellipsis: true,
    });
  }

  // Caixa direita: título DACTE
  const tituloH = 26;
  box(dirX, y, dirW, tituloH);
  doc
    .fontSize(9)
    .font('Helvetica-Bold')
    .text('DACTE', dirX + 3, y + 3, { width: dirW - 6 });
  doc
    .fontSize(5.5)
    .font('Helvetica')
    .text(
      'Documento Auxiliar do Conhecimento de Transporte Eletrônico',
      dirX + 3,
      y + 14,
      { width: dirW - 6 },
    );

  // Linha impressão + modelo
  const impH = 12;
  const impY = y + tituloH;
  const impHalf = dirW / 2;
  cell(dirX, impY, impHalf, impH, 'MODELO', txt(data.modelo) || '57', {
    valueSize: 7,
  });
  cell(dirX + impHalf, impY, dirW - impHalf, impH, 'IMPRESSÃO', dataImpressao, {
    valueSize: 6,
  });

  // Grade SÉRIE / NÚMERO / MODAL / Nº PROTOCOLO
  const gradeY = impY + impH;
  const gradeH = headerH - tituloH - impH;
  const cSerie = dirW * 0.16;
  const cNum = dirW * 0.26;
  const cModal = dirW * 0.22;
  const cProt = dirW - cSerie - cNum - cModal;
  let gx = dirX;
  cell(gx, gradeY, cSerie, gradeH, 'SÉRIE', txt(data.serie), { align: 'center' });
  gx += cSerie;
  cell(gx, gradeY, cNum, gradeH, 'NÚMERO', txt(data.numero), { align: 'center' });
  gx += cNum;
  cell(gx, gradeY, cModal, gradeH, 'MODAL', txt(data.modal), { align: 'center' });
  gx += cModal;
  cell(gx, gradeY, cProt, gradeH, 'Nº PROTOCOLO', txt(data.protocolo), {
    align: 'center',
    valueSize: 6,
  });

  y += headerH;

  // ===========================================================================
  // 2) Linha CNPJ / IE / RNTRC do emitente
  // ===========================================================================
  const idH = 14;
  const idCnpj = width * 0.4;
  const idIe = width * 0.3;
  const idRntrc = width - idCnpj - idIe;
  cell(left, y, idCnpj, idH, 'CNPJ', txt(emit?.cnpjCpf));
  cell(left + idCnpj, y, idIe, idH, 'INSCRIÇÃO ESTADUAL', txt(emit?.ie));
  cell(left + idCnpj + idIe, y, idRntrc, idH, 'RNTRC', txt(data.rntrc));
  y += idH;

  // ===========================================================================
  // 3) CONTROLE DO FISCO + CHAVE DE ACESSO + código de barras
  // ===========================================================================
  const fiscoH = 50;
  box(left, y, width, fiscoH);
  doc
    .fontSize(5.5)
    .font('Helvetica')
    .text('CONTROLE DO FISCO', left + 3, y + 2, { width: width - 6 });

  // Código de barras Code128 simplificado (representação visual a partir dos dígitos).
  const chaveDigits = (data.chave || '').replace(/\D/g, '');
  desenharCodeBars(doc, chaveDigits, left + 4, y + 12, width - 8, 18);

  // Chave de acesso formatada
  doc
    .fontSize(5)
    .font('Helvetica')
    .text('CHAVE DE ACESSO', left + 3, y + 32, { width: width - 6 });
  doc
    .fontSize(7)
    .font('Helvetica-Bold')
    .text(formatarChave(data.chave), left + 3, y + 39, {
      width: width - 6,
      align: 'center',
    });
  doc.font('Helvetica');
  y += fiscoH;

  // ===========================================================================
  // 4) TIPO CT-E / TIPO SERVIÇO / CFOP - NATUREZA
  // ===========================================================================
  const tipoH = 14;
  const tA = width * 0.25;
  const tB = width * 0.25;
  const tC = width - tA - tB;
  cell(left, y, tA, tipoH, 'TIPO DO CT-E', txt(data.tipoCte));
  cell(left + tA, y, tB, tipoH, 'TIPO DO SERVIÇO', txt(data.tipoServico));
  cell(
    left + tA + tB,
    y,
    tC,
    tipoH,
    'CFOP - NATUREZA DA PRESTAÇÃO',
    `${txt(data.cfop)} ${txt(data.naturezaOperacao)}`.trim(),
    { valueSize: 6 },
  );
  y += tipoH;

  // ===========================================================================
  // 5) ORIGEM / DESTINO / EMITIDO POR
  // ===========================================================================
  const odH = 14;
  const odA = width * 0.37;
  const odB = width * 0.37;
  const odC = width - odA - odB;
  cell(
    left,
    y,
    odA,
    odH,
    'ORIGEM DA PRESTAÇÃO',
    `${txt(data.origemMunicipio)}/${txt(data.origemUf)}`,
  );
  cell(
    left + odA,
    y,
    odB,
    odH,
    'DESTINO DA PRESTAÇÃO',
    `${txt(data.destinoMunicipio)}/${txt(data.destinoUf)}`,
  );
  cell(left + odA + odB, y, odC, odH, 'EMITIDO POR', txt(data.emitidoPor));
  y += odH;

  // ===========================================================================
  // 6) Partes: REMETENTE, DESTINATÁRIO, EXPEDIDOR, RECEBEDOR
  // ===========================================================================
  const drawParte = (rotulo: string, p: CtePartelE | null) => {
    if (!p) return;
    const ph = 30;
    box(left, y, width, ph);
    doc
      .fontSize(5.5)
      .font('Helvetica')
      .fillColor('#000000')
      .text(rotulo, left + 3, y + 1.5, { width: width - 6 });
    textIn(left, y + 8, width, txt(p.nome), 7, true);
    textIn(left, y + 16, width, enderecoCompleto(p), 6, false);
    const linha = [
      'CNPJ/CPF ' + txt(p.cnpjCpf),
      'IE ' + txt(p.ie),
      'FONE ' + txt(p.fone),
    ].join('   ');
    textIn(left, y + 23, width, linha, 6, false);
    y += ph;
  };

  drawParte('REMETENTE', data.remetente);
  drawParte('DESTINATÁRIO', data.destinatario);
  drawParte('EXPEDIDOR', data.expedidor);
  drawParte('RECEBEDOR', data.recebedor);

  // ===========================================================================
  // 7) COMPONENTES DO FRETE + valores
  // ===========================================================================
  const compH = 36;
  const compW = width * 0.55;
  const valW = width - compW;

  // Bloco componentes (esq)
  box(left, y, compW, compH);
  doc
    .fontSize(5.5)
    .font('Helvetica')
    .fillColor('#000000')
    .text('COMPONENTES DO VALOR DA PRESTAÇÃO DO SERVIÇO', left + 3, y + 1.5, {
      width: compW - 6,
    });
  const comps: CteComponente[] = Array.isArray(data.componentes)
    ? data.componentes
    : [];
  let cy = y + 9;
  const maxComps = 4;
  if (comps.length === 0) {
    doc.fontSize(6).font('Helvetica').text('-', left + 3, cy, {
      width: compW - 6,
    });
  } else {
    for (const c of comps.slice(0, maxComps)) {
      doc
        .fontSize(6)
        .font('Helvetica')
        .text(txt(c.nome), left + 3, cy, {
          width: compW * 0.6,
          ellipsis: true,
        });
      doc.text(brl(c.valor), left + compW * 0.6, cy, {
        width: compW * 0.4 - 4,
        align: 'right',
      });
      cy += 6;
    }
    if (comps.length > maxComps) {
      doc
        .fontSize(5)
        .font('Helvetica')
        .text(`(+${comps.length - maxComps} componentes)`, left + 3, cy, {
          width: compW - 6,
        });
    }
  }

  // Bloco valores (dir)
  const valHalf = compH / 2;
  cell(
    left + compW,
    y,
    valW,
    valHalf,
    'VALOR TOTAL DA PRESTAÇÃO DO SERVIÇO',
    brl(data.valorTotalPrestacao),
    { align: 'right', valueSize: 8 },
  );
  cell(
    left + compW,
    y + valHalf,
    valW,
    compH - valHalf,
    'VALOR A RECEBER',
    brl(data.valorReceber),
    { align: 'right', valueSize: 8 },
  );
  y += compH;

  // ===========================================================================
  // 8) INFORMAÇÕES RELATIVAS AO ICMS
  // ===========================================================================
  const icmsHeaderH = 9;
  box(left, y, width, icmsHeaderH);
  doc
    .fontSize(5.5)
    .font('Helvetica')
    .fillColor('#000000')
    .text('INFORMAÇÕES RELATIVAS AO ICMS', left + 3, y + 1.5, {
      width: width - 6,
    });
  y += icmsHeaderH;

  const icmsH = 14;
  const iA = width * 0.25;
  const iB = width * 0.25;
  const iC = width * 0.25;
  const iD = width - iA - iB - iC;
  cell(left, y, iA, icmsH, 'SITUAÇÃO TRIBUTÁRIA (CST)', txt(data.cst), {
    align: 'center',
  });
  cell(left + iA, y, iB, icmsH, 'BASE DE CÁLCULO', brl(data.icmsBase), {
    align: 'right',
  });
  cell(
    left + iA + iB,
    y,
    iC,
    icmsH,
    'ALÍQ. ICMS (%)',
    num(data.icmsAliquota),
    { align: 'right' },
  );
  cell(left + iA + iB + iC, y, iD, icmsH, 'VALOR ICMS', brl(data.icmsValor), {
    align: 'right',
  });
  y += icmsH;

  // ===========================================================================
  // 9) DADOS DA CARGA
  // ===========================================================================
  const cargaH = 14;
  const caA = width * 0.6;
  const caB = width - caA;
  cell(
    left,
    y,
    caA,
    cargaH,
    'PRODUTO PREDOMINANTE',
    txt(data.produtoPredominante),
  );
  cell(left + caA, y, caB, cargaH, 'VALOR TOTAL DA CARGA', brl(data.valorCarga), {
    align: 'right',
  });
  y += cargaH;

  // ===========================================================================
  // 10) DESTAQUE DE TRIBUTOS (Lei 12.741/2012)
  // ===========================================================================
  const tribH = 12;
  cell(
    left,
    y,
    width,
    tribH,
    'VALOR APROXIMADO DOS TRIBUTOS (Lei 12.741/2012)',
    brl(data.valorTotalTributos),
    { align: 'right' },
  );
  y += tribH;

  // ===========================================================================
  // 11) OBSERVAÇÕES
  // ===========================================================================
  const obsH = 22;
  box(left, y, width, obsH);
  doc
    .fontSize(5.5)
    .font('Helvetica')
    .fillColor('#000000')
    .text('OBSERVAÇÕES', left + 3, y + 1.5, { width: width - 6 });
  doc
    .fontSize(6)
    .font('Helvetica')
    .text(txt(data.observacoes), left + 3, y + 8, {
      width: width - 6,
      height: obsH - 9,
      ellipsis: true,
    });
  y += obsH;

  // ===========================================================================
  // 12) CHAVES NF-E referenciadas
  // ===========================================================================
  const nfeH = 24;
  box(left, y, width, nfeH);
  doc
    .fontSize(5.5)
    .font('Helvetica')
    .fillColor('#000000')
    .text('DOCUMENTOS ORIGINÁRIOS — CHAVES NF-E', left + 3, y + 1.5, {
      width: width - 6,
    });
  const nfes: string[] = Array.isArray(data.documentosNFe)
    ? data.documentosNFe
    : [];
  if (nfes.length === 0) {
    doc.fontSize(6).font('Helvetica').text('-', left + 3, y + 8, {
      width: width - 6,
    });
  } else {
    const colW = width / 2 - 4;
    const maxNfe = 6;
    nfes.slice(0, maxNfe).forEach((ch, i) => {
      const col = i % 2;
      const row = Math.floor(i / 2);
      const nx = left + 3 + col * (width / 2);
      const ny = y + 8 + row * 5;
      doc
        .fontSize(5.5)
        .font('Helvetica')
        .text(formatarChave(ch).replace(/\s/g, ''), nx, ny, {
          width: colW,
          ellipsis: true,
        });
    });
    if (nfes.length > maxNfe) {
      doc
        .fontSize(5)
        .font('Helvetica')
        .text(`(+${nfes.length - maxNfe} chaves)`, left + 3, y + nfeH - 6, {
          width: width - 6,
        });
    }
  }
  // y final intencionalmente não usado adiante.
}

// ----------------------------------------------------------------------------
// Código de barras (representação simplificada a partir dos dígitos)
// ----------------------------------------------------------------------------

/**
 * Desenha uma representação visual de código de barras tipo Code128 a partir
 * dos dígitos fornecidos. Não é um Code128 decodificável de verdade — é uma
 * representação gráfica determinística (barras de larguras variadas conforme os
 * dígitos), suficiente para o aspecto do DACTE.
 */
function desenharCodeBars(
  doc: any,
  digits: string,
  x: number,
  y: number,
  maxWidth: number,
  height: number,
): void {
  if (!digits) {
    doc.fontSize(6).font('Helvetica').text('-', x, y);
    return;
  }
  doc.save().fillColor('#000000');

  // Cada dígito vira um padrão de 4 módulos (barra/espaço), largura 1..4.
  type Mod = { bar: boolean; w: number };
  const mods: Mod[] = [];
  for (const ch of digits) {
    const d = parseInt(ch, 10);
    const safe = isNaN(d) ? 0 : d;
    mods.push({ bar: true, w: ((safe % 3) + 1) }); // 1..3
    mods.push({ bar: false, w: (((safe + 1) % 2) + 1) }); // 1..2
    mods.push({ bar: true, w: (((safe + 2) % 2) + 1) }); // 1..2
    mods.push({ bar: false, w: 1 });
  }

  const totalUnits = mods.reduce((s, m) => s + m.w, 0);
  const unit = totalUnits > 0 ? Math.max(0.4, maxWidth / totalUnits) : 0.5;

  let cx = x;
  for (const m of mods) {
    const w = m.w * unit;
    if (m.bar) {
      doc.rect(cx, y, w, height).fill('#000000');
    }
    cx += w;
  }
  doc.restore();
}
