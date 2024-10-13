# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

@dlt.table (
    name = "aposentado",
    comment = "Tabela que armazena dados de cadastro dos aposentados",
    partition_cols=["ano_mes"],
    schema="""
        numero_cpf STRING NOT NULL,
        identificador_pessoa INT NOT NULL,        
        nome_pessoa STRING NOT NULL,        
        numero_matricula STRING NOT NULL,
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
        ano_mes INT,
        CONSTRAINT Aposentado_PK PRIMARY KEY (numero_cpf)
    """
)
def aposentado():
    # Leitura dos dados
    df = spark.read.format("delta").load('/silver/SIAPE/Aposentados/Cadastro')

    # Se a tabela ja existe, le os dados existentes e filtra para nao inserir particoes duplicadas
    try:        
        existing_data = dlt.read("aposentado")       
        existing_partitions = existing_data.select("ano_mes").distinct()        
        new_data = df.join(existing_partitions, "ano_mes", "leftanti")         
        return existing_data.union(new_data)
    except:
        # Se a tabela nao existe, retorna os dados novos        
        return df



# COMMAND ----------

@dlt.table (
    name = "servidor",
    comment = "Tabela que armazena dados de cadastro dos servidores",
    partition_cols=["ano_mes"],
    schema="""
        numero_cpf STRING NOT NULL,
        identificador_pessoa INT NOT NULL,        
        nome_pessoa STRING NOT NULL,        
        numero_matricula STRING NOT NULL,
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
        ano_mes INT,
        CONSTRAINT Servidor_PK PRIMARY KEY (numero_cpf)
    """
)
def servidor():
    # Leitura dos dados
    df = spark.read.format("delta").load('/silver/SIAPE/Servidores/Cadastro')

    # Se a tabela ja existe, le os dados existentes e filtra para nao inserir particoes duplicadas
    try:        
        existing_data = dlt.read("servidor")       
        existing_partitions = existing_data.select("ano_mes").distinct()        
        new_data = df.join(existing_partitions, "ano_mes", "leftanti")

        return existing_data.union(new_data)
    except:
        # Se a tabela nao existe, retorna os dados novos        
        return df

# COMMAND ----------

@dlt.table (
    name = "pensionista",
    comment = "Tabela que armazena dados de cadastro dos pensionistas",
    partition_cols=["ano_mes"],
    schema="""
        numero_cpf STRING NOT NULL,
        identificador_pessoa INT NOT NULL,        
        nome_pessoa STRING NOT NULL,        
        numero_matricula STRING NOT NULL,
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
        ano_mes INT,
        CONSTRAINT Pensionista_PK PRIMARY KEY (numero_cpf)
    """
)
def pensionista():
    # Leitura dos dados
    df = spark.read.format("delta").load('/silver/SIAPE/Pensionistas/Cadastro')

    # Se a tabela ja existe, le os dados existentes e filtra para nao inserir particoes duplicadas
    try:        
        existing_data = dlt.read("pensionista")       
        existing_partitions = existing_data.select("ano_mes").distinct()        
        new_data = df.join(existing_partitions, "ano_mes", "leftanti") 

        return existing_data.union(new_data)
    except:
        # Se a tabela nao existe, retorna os dados novos
        return df

# COMMAND ----------

@dlt.table (
    name = "remuneracao",
    comment = "Tabela que armazena dados de remuneracao dos servidores, aposentados e pensionistas",
    partition_cols=["ano_mes"],
    schema="""
        numero_cpf STRING NOT NULL,
        codigo_categoria_remuneracao INT NOT NULL,
        identificador_pessoa INT NOT NULL,
        nome_pessoa STRING NOT NULL,
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
        CONSTRAINT Remuneracao_PK PRIMARY KEY (numero_cpf, codigo_categoria_remuneracao)
    """
)
def remuneracao():
    # Leitura dos dados
    df_servidores = spark.read.format("delta").load('/silver/SIAPE/Servidores/Remuneracao')
    df_aposentados = spark.read.format("delta").load('/silver/SIAPE/Aposentados/Remuneracao')
    df_pensionistas = spark.read.format("delta").load('/silver/SIAPE/Pensionistas/Remuneracao')

    df_union = df_servidores.union(df_aposentados).union(df_pensionistas)

    # Se a tabela ja existe, le os dados existentes e filtra para nao inserir particoes duplicadas
    try:        
        delta_table = dlt.read("remuneracao")
        delta_table.alias("delta_table").merge(df_union.alias("df_union"), "delta_table.ano_mes = df_union.ano_mes").whenNotMatchedInsertAll().execute()        
    except:        
        # Se a tabela nao existe, adiciona os dados novos        
        return df_union
