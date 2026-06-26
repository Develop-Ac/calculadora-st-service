import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { CteRastreioService } from './cte-rastreio.service';

/**
 * Polling periódico do rastreio de CT-e no SSW (trackingdanfe).
 *
 * Config por env:
 *  - SSW_TRACKING_CRON: expressão cron (default a cada 30 min).
 *  - SSW_TRACKING_CRON_DISABLED=true: desliga o disparo periódico.
 *
 * Mantenha DISABLED=true até aplicar o DDL das novas colunas/tabelas no Postgres.
 */
@Injectable()
export class CteRastreioCron {
  private readonly logger = new Logger(CteRastreioCron.name);
  private rodando = false;

  constructor(private readonly service: CteRastreioService) {}

  @Cron(process.env.SSW_TRACKING_CRON || '*/30 * * * *', { name: 'cte-rastreio' })
  async sync() {
    if (process.env.SSW_TRACKING_CRON_DISABLED === 'true') return;
    if (this.rodando) {
      this.logger.warn('Rastreio de CT-e anterior ainda em execução; pulando este disparo.');
      return;
    }

    this.rodando = true;
    try {
      await this.service.sincronizar(); // o resumo é logado pelo próprio serviço
    } catch (err) {
      this.logger.error(
        `Falha no rastreio de CT-e: ${err instanceof Error ? err.message : String(err)}`,
      );
    } finally {
      this.rodando = false;
    }
  }
}
