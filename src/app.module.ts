import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { IcmsModule } from './icms/icms.module';
import { NfseModule } from './nfse/nfse.module';
import { PrismaModule } from './prisma/prisma.module';
import { OpenQueryModule } from './shared/database/openquery/openquery.module';

@Module({
    imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        ScheduleModule.forRoot(),
        PrismaModule,
        OpenQueryModule,
        IcmsModule,
        NfseModule,
    ],
})
export class AppModule { }
