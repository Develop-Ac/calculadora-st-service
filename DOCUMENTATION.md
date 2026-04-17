# Documentação de Desenvolvimento - Calculadora ICMS ST Service

Este documento serve como guia para o desenvolvimento e manutenção da API `calculadora-st-service`.

## Visão Geral

O serviço é responsável por:
1.  Calcular o ICMS Sutbstituição Tributária (ST) e Diferencial de Alíquota (DIFAL) conforme a Portaria 195/2019 (MT).
2.  Gerar PDFs da DANFE a partir do XML da nota fiscal.
3.  Fornecer endpoints para conciliação de notas fiscais de entrada.

## Stack Tecnológica

*   **Framework**: NestJS (Node.js)
*   **Linguagem**: TypeScript
*   **Banco de Dados**: Postgres (acessado via Prisma ORM)
*   **Documentação**: Swagger (OpenAPI)
*   **Libs Principais**:
    *   `@alexssmusica/node-pdf-nfe`: Geração de DANFE
    *   `archiver`: Compactação ZIP
    *   `xml2js`: Parse de XML

## Configuração do Ambiente

Certifique-se de ter instalado:
*   Node.js (v18 ou superior)
*   NPM
*   Docker (opcional, para facilitar deploy)

### 1. Instalação de Dependências

```bash
npm install
```

### 2. Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto com as seguintes chaves (exemplo):

```env
DATABASE_URL="sqlserver://host:port;database=db;user=user;password=pass;encrypt=true;trustServerCertificate=true"
PORT=3001
CORS_ORIGIN=http://localhost:3000,http://cotacao-frontend.acacessorios.local
```

### 3. Prisma

Se houver alterações no schema do banco de dados (`prisma/schema.prisma`):

```bash
npx prisma generate
```

## Execução

### Desenvolvimento

```bash
npm run dev
```

O servidor iniciará em `http://localhost:3001` (ou a porta definida no .env).

### Produção

```bash
npm run build
npm run start:prod
```

## Documentação da API (Swagger)

A API possui documentação automática gerada pelo Swagger.

*   Acesse: **`/api/docs`** (ex: `http://localhost:3001/api/docs`)
*   Aqui você pode ver todos os endpoints disponíveis, seus parâmetros e testar requisições diretamente pelo navegador.

### Status do Servidor

Ao acessar a raiz (`/`), o servidor retorna um JSON indicando status online:

```json
{
  "status": "online",
  "message": "O servidor está online e funcional",
  "docs": "/api/docs",
  "timestamp": "..."
}
```

## Estrutura do Projeto

*   `src/main.ts`: Ponto de entrada. Configuração do Swagger e CORS.
*   `src/app.module.ts`: Módulo raiz.
*   `src/icms/`: Módulo principal de cálculo de ICMS.
    *   `icms.controller.ts`: Definição das rotas (`/icms/*`).
    *   `icms.service.ts`: Lógica de negócio (cálculos e geração de PDF).
*   `src/prisma/`: Configuração do cliente ORM.

## Fluxos Principais

### Cálculo de ST
1.  Frontend envia XMLs ou chaves de nota.
2.  Backend processa cada item do XML.
3.  Verifica NCM, aplica MVA (Margem de Valor Agregado).
4.  Compara ST destacado na nota com o calculado.
5.  Retorna divergências e valores a recolher.

### Geração de DANFE
1.  Endpoint `/icms/danfe` recebe o XML completo.
2.  Utiliza a lib `@alexssmusica/node-pdf-nfe` para gerar o PDF em memória.
3.  Retorna o stream do arquivo PDF.

## Manutenção Futura

*   **Adicionar novas rotas**: Crie novos métodos no `IcmsController` e decore com `@Get`, `@Post`, etc. O Swagger detectará automaticamente.
*   **Alterar regras de cálculo**: Edite `icms.service.ts`.
*   **Atualizar dependências**: Execute `npm update`. Teste sempre a geração de PDF após updates, pois libs de PDF podem ser sensíveis.

## Conciliação Fiscal por Item (abril/2026)

Foi adicionada a trilha de conferência fiscal por item da nota com vínculo de cadastro entre fornecedor e produto interno.

### Estrutura de banco (PostgreSQL)

Aplicar manualmente o script:

`sql/2026-04-17_conciliacao_fiscal_item.sql`

Esse script:

* adiciona em `com_nfe_conciliacao` os campos:
  * `compra_comercializacao`
  * `uso_consumo`
* cria a tabela `com_nfe_conciliacao_item` para persistência por item da conferência fiscal.

### Novo endpoint

* `POST /icms/fiscal-conferencia/preview`

Entrada:

```json
{
  "notas": [
    {
      "chaveNfe": "...",
      "itens": [
        {
          "item": 1,
          "codProdFornecedor": "123",
          "impostoEscolhido": "ST",
          "destinacaoMercadoria": "COMERCIALIZACAO",
          "ncmNota": "8708...",
          "cstNota": "060"
        }
      ]
    }
  ]
}
```

Saída:

* flags por nota (`compraComercializacao`, `usoConsumo`)
* status por item (`OK` ou `DIVERGENTE`)
* lista de divergências para conferência de cadastro.

### Regras implementadas

1. Vínculo fornecedor/produto:
   * `CPF_CNPJ` do emitente na `Stage_Fornecedores` para obter `FOR_CODIGO`.
   * `FOR_CODIGO + COD_PROD_FORNECEDOR` na `Stage_Produtos_Fornecedor_NFE` para obter `PRO_CODIGO`.

2. Comercialização:
   * quando item marcado com ST: `ST_CODIGO = ST0-X`.
   * `SUBTIPO = 00`.
   * NCM monofásico (match completo, raiz 6 ou raiz 4): `PIS_CODIGO=04` e `COFINS_CODIGO=04`.
   * não monofásico: `PIS_CODIGO=P01` e `COFINS_CODIGO=C01`.
   * compra interna com ST: CST da nota precisa terminar em `10` ou `60`.

3. Uso e consumo:
   * `COMERCIALIZAVEL = N`
   * `PIS_CODIGO = P99`
   * `COFINS_CODIGO = C99`
   * `SUBGRP_CODIGO = 274`
   * `SUBTIPO = 07`

### Observação operacional

O endpoint `POST /icms/payment-status` passou a aceitar a coleção de itens para, além do status da nota, persistir a conferência fiscal por item quando a estrutura SQL já estiver aplicada.
