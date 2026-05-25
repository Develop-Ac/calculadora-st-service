-- Adiciona a coluna sem_tributacao na tabela de itens de conciliação fiscal.
-- Execute este script diretamente no banco de dados antes de utilizar a nova versão do serviço.

ALTER TABLE com_nfe_conciliacao_item
    ADD COLUMN IF NOT EXISTS sem_tributacao BOOLEAN NOT NULL DEFAULT FALSE;
