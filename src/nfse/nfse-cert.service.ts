import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { createSecureContext } from 'tls';
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

    if (!temSegredo()) {
      this.logger.warn('NFSE_CERT_SECRET ausente: a senha do certificado será guardada em TEXTO PURO.');
    }

    const saved = await this.prisma.nfseCertificado.upsert({
      where: { cnpj },
      create: {
        cnpj,
        nome: params.nome || null,
        arquivo: params.pfx,
        senha: cifrar(params.senha),
        validade_ate: params.validadeAte || null,
        ativo: true,
      },
      update: {
        nome: params.nome || null,
        arquivo: params.pfx,
        senha: cifrar(params.senha),
        validade_ate: params.validadeAte || null,
        ativo: true,
      },
    });
    this.logger.log(`Certificado vinculado para o CNPJ ${cnpj}.`);
    return { cnpj: saved.cnpj, nome: saved.nome || undefined, validade_ate: saved.validade_ate };
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
