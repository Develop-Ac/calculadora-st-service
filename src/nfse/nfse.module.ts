import { Module } from '@nestjs/common';
import { NfseController } from './nfse.controller';
import { NfseDistService } from './nfse-dist.service';
import { NfseCertService } from './nfse-cert.service';
import { NfseAdnClient } from './nfse-adn.client';
import { NfseDistCron } from './nfse-dist.cron';

@Module({
  controllers: [NfseController],
  providers: [NfseDistService, NfseCertService, NfseAdnClient, NfseDistCron],
})
export class NfseModule {}
