import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { createSecureContext } from 'tls';
import * as forge from 'node-forge';
import { PrismaService } from '../prisma/prisma.service';
import { cifrar, decifrar, temSegredo } from './nfse-crypto.util';

export interface CertConfig {
  cnpj: string;
  pfx: Buffer;
  passphrase: string;
}

/**
 * Gestão do certificado e-CNPJ A1 usado no mTLS da ADN.
 * O .pfx fica em com_nfse_certificado (BYTEA) e a senha é cifrada.
 * Vinculado pelo frontend (upload).
 */
@Injectable()
export class NfseCertService {
  private readonly logger = new Logger(NfseCertService.name);

  constructor(private readonly prisma: PrismaService) {}

  /** Salva/atualiza o certificado, validando .pfx + senha antes. */
  async salvar(params: {
    cnpj: string;
    nome?: string;
    pfx: Buffer;
    senha: string;
    validadeAte?: Date | null;
  }): Promise<{ cnpj: string; nome?: string; validade_ate?: Date | null }> {
    const cnpj = (params.cnpj || '').replace(/\D/g, '');
    if (cnpj.length !== 14) throw new BadRequestException('CNPJ inválido (14 dígitos).');
    if (!params.pfx?.length) throw new BadRequestException('Arquivo .pfx não enviado.');

    // Valida o par .pfx/senha: createSecureContext lança se a senha estiver errada.
    try {
      createSecureContext({ pfx: params.pfx, passphrase: params.senha });
    } catch {
      throw new BadRequestException('Certificado ou senha inválidos (não foi possível abrir o .pfx).');
    }

    // Extrai Razão Social + validade do certificado (best-effort, não lança).
    const dados = this.extrairDados(params.pfx, params.senha, cnpj);
    const nome = dados.razaoSocial || params.nome || null;
    const validadeAte = params.validadeAte || dados.validadeAte || null;

    if (!temSegredo()) {
      this.logger.warn('NFSE_CERT_SECRET ausente: a senha do certificado será guardada em TEXTO PURO.');
    }

    const saved = await this.prisma.nfseCertificado.upsert({
      where: { cnpj },
      create: {
        cnpj,
        nome,
        arquivo: params.pfx,
        senha: cifrar(params.senha),
        validade_ate: validadeAte,
        ativo: true,
      },
      update: {
        nome,
        arquivo: params.pfx,
        senha: cifrar(params.senha),
        validade_ate: validadeAte,
        ativo: true,
      },
    });
    this.logger.log(`Certificado vinculado: ${cnpj}${nome ? ` (${nome})` : ''}.`);
    return { cnpj: saved.cnpj, nome: saved.nome || undefined, validade_ate: saved.validade_ate };
  }

  /**
   * Lê o .pfx (PKCS#12) e extrai a Razão Social e a validade do certificado do
   * titular. No e-CNPJ ICP-Brasil o Subject CN é "RAZÃO SOCIAL:CNPJ".
   * Best-effort: nunca lança (retorna {} em caso de falha).
   */
  private extrairDados(
    pfx: Buffer,
    senha: string,
    cnpjEsperado?: string,
  ): { razaoSocial?: string; cnpj?: string; validadeAte?: Date } {
    try {
      const asn1 = forge.asn1.fromDer(forge.util.createBuffer(pfx.toString('binary')));
      const p12 = forge.pkcs12.pkcs12FromAsn1(asn1, false, senha);
      const bags = p12.getBags({ bagType: forge.pki.oids.certBag });
      const certBags: any[] = (bags[forge.pki.oids.certBag] as any) || [];
      const esperado = (cnpjEsperado || '').replace(/\D/g, '');

      // Escolhe o certificado do TITULAR (CN "RAZÃO:CNPJ"), não o da AC.
      let titular: any;
      let candidato: any;
      for (const b of certBags) {
        const cert = b?.cert;
        if (!cert) continue;
        const cn = String(cert.subject.getField('CN')?.value || '');
        const digitos = cn.replace(/\D/g, '');
        if (esperado && digitos.includes(esperado)) {
          titular = cert;
          break;
        }
        if (!candidato && cn.includes(':')) candidato = cert;
      }
      const cert = titular || candidato || certBags[0]?.cert;
      if (!cert) return {};

      const cn = String(cert.subject.getField('CN')?.value || '');
      const [razao, cnpjRaw] = cn.split(':');
      const cnpj = (cnpjRaw || '').replace(/\D/g, '');
      return {
        razaoSocial: (razao || cn).trim() || undefined,
        cnpj: cnpj.length === 14 ? cnpj : undefined,
        validadeAte: cert.validity?.notAfter,
      };
    } catch (e) {
      this.logger.warn(
        `Não foi possível extrair dados do certificado: ${e instanceof Error ? e.message : String(e)}`,
      );
      return {};
    }
  }

  /** Retorna o certificado ativo (preferindo o do CNPJ, senão o mais recente). */
  async obterAtivo(cnpjPreferido?: string): Promise<CertConfig | null> {
    const cnpj = (cnpjPreferido || '').replace(/\D/g, '');
    const cert = cnpj
      ? await this.prisma.nfseCertificado.findFirst({ where: { cnpj, ativo: true } })
      : null;
    const escolhido =
      cert ||
      (await this.prisma.nfseCertificado.findFirst({
        where: { ativo: true },
        orderBy: { atualizado_em: 'desc' },
      }));
    if (!escolhido) return null;
    return { cnpj: escolhido.cnpj, pfx: Buffer.from(escolhido.arquivo), passphrase: decifrar(escolhido.senha) };
  }

  /** Backfill: re-extrai a Razão Social/validade dos certificados já vinculados
   *  (usa o .pfx guardado + senha decifrada). Corrige nomes gravados como arquivo. */
  async reextrairNomes(): Promise<{ total: number; atualizados: number }> {
    const certs = await this.prisma.nfseCertificado.findMany();
    let atualizados = 0;
    for (const c of certs) {
      try {
        const senha = decifrar(c.senha);
        const dados = this.extrairDados(Buffer.from(c.arquivo), senha, c.cnpj);
        if (dados.razaoSocial && dados.razaoSocial !== c.nome) {
          await this.prisma.nfseCertificado.update({
            where: { cnpj: c.cnpj },
            data: { nome: dados.razaoSocial, validade_ate: c.validade_ate || dados.validadeAte || null },
          });
          atualizados++;
        }
      } catch {
        /* pula este cert */
      }
    }
    this.logger.log(`Re-extração de nomes de certificado: ${atualizados}/${certs.length} atualizado(s).`);
    return { total: certs.length, atualizados };
  }

  /** CNPJs (14 díg.) de todos os certificados ativos — empresas a captar. */
  async listarCnpjsAtivos(): Promise<string[]> {
    const certs = await this.prisma.nfseCertificado.findMany({
      where: { ativo: true },
      select: { cnpj: true },
      orderBy: { atualizado_em: 'desc' },
    });
    return certs.map((c) => c.cnpj.replace(/\D/g, '')).filter((c) => c.length === 14);
  }

  /** Metadados (sem expor o .pfx nem a senha) para a tela. */
  async status() {
    const certs = await this.prisma.nfseCertificado.findMany({
      orderBy: { atualizado_em: 'desc' },
      select: { cnpj: true, nome: true, validade_ate: true, ativo: true, atualizado_em: true },
    });
    return { vinculado: certs.length > 0, certificados: certs };
  }

  async remover(cnpj: string) {
    const c = (cnpj || '').replace(/\D/g, '');
    await this.prisma.nfseCertificado.deleteMany({ where: { cnpj: c } });
    return { success: true, cnpj: c };
  }
}
