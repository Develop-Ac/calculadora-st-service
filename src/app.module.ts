import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { IcmsModule } from './icms/icms.module';
import { PrismaModule } from './prisma/prisma.module';
import { OpenQueryModule } from './shared/database/openquery/openquery.module';

@Module({
    imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        PrismaModule,
        OpenQueryModule,
        IcmsModule,
    ],
})
export class AppModule { }
