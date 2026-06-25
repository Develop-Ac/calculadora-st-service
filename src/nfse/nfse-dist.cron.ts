import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { NfseDistService } from './nfse-dist.service';

/**
 * Polling periódico da Distribuição de NFS-e (ADN).
 *
 * Config por env:
 *  - NFSE_DIST_CRON: expressão cron (default a cada 15 min).
 *  - NFSE_DIST_CRON_DISABLED=true: desliga o disparo periódico.
 *
 * Recomenda-se manter DISABLED=true até validar o contrato real da ADN em
 * produção restrita (com o certificado A1) e confirmar os nomes dos campos.
 */
@Injectable()
export class NfseDistCron {
  private readonly logger = new Logger(NfseDistCron.name);
  private rodando = false;

  constructor(private readonly service: NfseDistService) {}

  @Cron(process.env.NFSE_DIST_CRON || '*/15 * * * *', { name: 'nfse-dist' })
  async sync() {
    if (process.env.NFSE_DIST_CRON_DISABLED === 'true') return;
    if (this.rodando) {
      this.logger.warn('Distribuição NFS-e anterior ainda em execução; pulando este disparo.');
      return;
    }

    this.rodando = true;
    try {
      await this.service.sincronizar(); // o resumo é logado pelo próprio serviço
    } catch (err) {
      this.logger.error(
        `Falha na distribuição NFS-e: ${err instanceof Error ? err.message : String(err)}`,
      );
    } finally {
      this.rodando = false;
    }
  }
}
