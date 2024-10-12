-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS picpay.public_informations;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS picpay.public_informations.pessoa (
    identificador_pessoa INT NOT NULL,
    nome_pessoa STRING NOT NULL,
    numero_cpf STRING NOT NULL,
    numero_matricula STRING NOT NULL,
    CONSTRAINT Pessoa_PK PRIMARY KEY (identificador_pessoa)
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS picpay.public_informations.servidor (
    identificador_pessoa INT NOT NULL,
    nome_cargo STRING,
    codigo_classe_cargo STRING,
    codigo_padrao_cargo STRING,
    codigo_nivel_cargo INT,
    codigo_sigla_funcao_comissionada STRING,
    codigo_nivel_funcao_comissionada INT,
    nome_funcao_comissionada STRING,
    codigo_atividade_comissionada INT,
    nome_atividade_comissionada STRING,
    codigo_opcao_parcial STRING,
    codigo_unidade_organizacional_lotacao BIGINT,
    nome_unidade_organizacional_lotacao STRING,
    codigo_orgao_lotacao INT,
    nome_orgao_lotacao STRING,
    codigo_orgao_superior_lotacao INT,
    nome_orgao_superior_lotacao STRING,
    codigo_unidade_organizacional_exercicio BIGINT,
    nome_unidade_organizacional_exercicio STRING,
    codigo_orgao_exercicio INT,
    nome_orgao_exercicio STRING,
    codigo_orgao_superior_exercicio INT,
    nome_orgao_superior_exercicio STRING,
    codigo_tipo_vinculo INT,
    nome_tipo_vinculo STRING,
    nome_situacao_vinculo STRING,
    data_inicio_afastamento DATE,
    data_termino_afastamento DATE,
    nome_regime_juridico STRING,
    nome_jornada_trabalho STRING,
    data_ingresso_cargo_funcao DATE,
    data_ingresso_orgao DATE,
    nome_documento_ingresso_servico_publico STRING,
    data_diploma_ingresso_servico_publico DATE,
    codigo_diploma_ingresso_orgao STRING,
    codigo_diploma_ingresso_servico_publico STRING,
    sigla_unidade_federacao_exercicio STRING,    
    CONSTRAINT Servidor_Pessoa_FK FOREIGN KEY (identificador_pessoa) 
    REFERENCES picpay.public_informations.pessoa(identificador_pessoa)    
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS picpay.public_informations.aposentado (
    identificador_pessoa INT NOT NULL,
    codigo_tipo_aposentadoria INT,
    nome_tipo_aposentadoria STRING,
    data_aposentadoria DATE,
    nome_cargo STRING,
    codigo_unidade_organizacional_lotacao BIGINT,
    nome_unidade_organizacional_lotacao STRING,
    codigo_orgao_lotacao INT,
    nome_orgao_lotacao STRING,
    codigo_orgao_superior_lotacao INT,
    nome_orgao_superior_lotacao STRING,
    codigo_tipo_vinculo INT,
    nome_tipo_vinculo STRING,
    nome_situacao_vinculo STRING,
    nome_regime_juridico STRING,
    nome_jornada_trabalho STRING,
    data_ingresso_cargo_funcao DATE,
    data_ingresso_orgao DATE,
    nome_documento_ingresso_servico_publico STRING,
    data_diploma_ingresso_servico_publico DATE,
    codigo_diploma_ingresso_orgao STRING,
    codigo_diploma_ingresso_servico_publico STRING,
    CONSTRAINT Aposentado_Pessoa_FK FOREIGN KEY (identificador_pessoa)
    REFERENCES picpay.public_informations.pessoa(identificador_pessoa)
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS picpay.public_informations.pensionista (
    identificador_pessoa INT NOT NULL,
    numero_cpf_representante_legal STRING,
    nome_representante_legal STRING,
    numero_cpf_instituidor_pensao STRING,
    nome_instituidor_pensao STRING,
    codigo_tipo_pensao INT,
    nome_tipo_pensao STRING,
    data_inicio_pensao DATE,
    nome_cargo_instituidor_pensao STRING,
    codigo_unidade_organizacional_lotacao_instituidor_pensao BIGINT,
    nome_unidade_organizacional_lotacao_instituidor_pensao STRING,
    codigo_orgao_lotacao_instituidor_pensao INT,
    nome_orgao_lotacao_instituidor_pensao STRING,
    codigo_orgao_superior_lotacao_instituidor_pensao INT,
    nome_orgao_superior_lotacao_instituidor_pensao STRING,
    codigo_tipo_vinculo INT,
    nome_tipo_vinculo STRING,
    nome_situacao_vinculo STRING,
    nome_regime_juridico_instituidor_pensao STRING,
    nome_jornada_trabalho_instituidor_pensao STRING,
    data_ingresso_cargo_funcao_instituidor_pensao DATE,
    data_ingresso_orgao_instituidor_pensao DATE,
    nome_documento_ingresso_servico_publico_instituidor_pensao STRING,
    data_diploma_ingresso_servico_publico_instituidor_pensao DATE,
    codigo_diploma_ingresso_orgao_instituidor_pensao STRING,
    codigo_diploma_ingresso_servico_publico_instituidor_pensao STRING,
    CONSTRAINT Pensionista_Pessoa_FK FOREIGN KEY (identificador_pessoa)
    REFERENCES picpay.public_informations.pessoa(identificador_pessoa)
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS picpay.public_informations.grupo (
    identificador_grupo INT NOT NULL PRIMARY KEY,
    nome_grupo STRING NOT NULL
);

INSERT INTO picpay.public_informations.grupo (identificador_grupo, nome_grupo)
VALUES (1, 'Servidor'), (2, 'Aposentado'), (3, 'Pensionista');

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS picpay.public_informations.remuneracao (
    identificador_pessoa INT NOT NULL,
    identificador_grupo INT NOT NULL,
    valor_remuneracao_basica_bruta DECIMAL(20,2),
    valor_deducao_abate_teto DECIMAL(20,2),
    valor_gratificacao_natalina DECIMAL(20,2),
    valor_deducao_abate_teto_gratificacao_natalina DECIMAL(20,2),
    valor_adicional_ferias DECIMAL(20,2),
    valor_remuneracao_eventual DECIMAL(20,2),    
    valor_deducao_irrf DECIMAL(20,2),
    valor_deducao_contribuicao_previdenciaria DECIMAL(20,2),
    valor_deducao_adiantamento DECIMAL(20,2),
    valor_deducao_pensao_militar DECIMAL(20,2),
    valor_deducao_fundo_saude DECIMAL(20,2),
    valor_deducao_taxa_ocupacao_imovel_funcional DECIMAL(20,2),
    valor_remuneracao_apos_deducao_obrigatoria DECIMAL(20,2),
    valor_verba_indenizatoria_rh_civil DECIMAL(20,2),
    valor_verba_indenizatoria_rh_militar DECIMAL(20,2),
    valor_verba_indenizatoria_desligamento_voluntario DECIMAL(20,2),
    valor_total_verba_indenizatoria DECIMAL(20,2),
    ano_mes INT,
    CONSTRAINT Remuneracao_PK PRIMARY KEY (identificador_pessoa, identificador_grupo),
    CONSTRAINT Remuneracao_Pessoa_FK FOREIGN KEY (identificador_pessoa) REFERENCES picpay.public_informations.pessoa(identificador_pessoa),
    CONSTRAINT Remuneracao_Grupo_FK FOREIGN KEY (identificador_grupo) REFERENCES picpay.public_informations.grupo(identificador_grupo)
)
PARTITIONED BY (ano_mes);
