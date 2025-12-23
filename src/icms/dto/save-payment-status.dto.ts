import { IsString, IsNotEmpty, IsNumber, IsOptional } from 'class-validator';

export class SavePaymentStatusDto {
    @IsString()
    @IsNotEmpty()
    chaveNfe: string;

    @IsNumber()
    @IsOptional()
    valor?: number;

    @IsString()
    @IsOptional()
    observacoes?: string;
}
