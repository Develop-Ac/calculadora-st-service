import { Module } from '@nestjs/common';
import { CteController } from './cte.controller';
import { CteService } from './cte.service';
import { CteRastreioClient } from './cte-rastreio.client';
import { CteRastreioService } from './cte-rastreio.service';
import { CteRastreioCron } from './cte-rastreio.cron';

@Module({
  controllers: [CteController],
  providers: [CteService, CteRastreioClient, CteRastreioService, CteRastreioCron],
})
export class CteModule {}
