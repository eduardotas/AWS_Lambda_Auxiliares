# AWS Lambda Auxiliares

![Lambda Image](https://github.com/eduardotas/AWS_Lambda_Auxiliares/assets/94205221/94ca367e-bafe-4147-b0d5-cc8e7503acdd)
![Lambda Image](https://github.com/eduardotas/AWS_Lambda_Auxiliares/assets/94205221/9887a0e3-0539-4282-a4e8-6677804020dc)

## Descrição

Este repositório contém informações sobre três AWS Lambdas criadas para auxiliar em um processo de ETL (Extração, Transformação e Carga) de dados. Cada Lambda desempenha uma função específica no pipeline de processamento de dados.

## Lambda 1: az-latam-brazil-auxiliares-im-s3-to-parquet

Esta Lambda é responsável por ler os flat-files do S3 raw, converter para o formato Parquet e gravá-los no S3 staging.

## Lambda 2: az-latam-brazil-auxiliares-im-parquet-to-redshift

Esta Lambda tem a função de carregar os dados do S3 staging e gravá-los no Redshift Data Warehouse (DS).

## Lambda 3: az-latam-brazil-auxiliares-im-redshift-to-sqlserver

A terceira Lambda executa stored procedures armazenadas no SQL Server. Cada SP lê os dados no Redshift Data Warehouse (DS) e os persiste no SQL Server.
