# AWS_Lambda_Auxiliares
![image](https://github.com/eduardotas/AWS_Lambda_Auxiliares/assets/94205221/94ca367e-bafe-4147-b0d5-cc8e7503acdd)
![image](https://github.com/eduardotas/AWS_Lambda_Auxiliares/assets/94205221/9887a0e3-0539-4282-a4e8-6677804020dc)

Lambda 1: az-latam-brazil-auxiliares-im-s3-to-parquet.
Etapa responsável por ler os flat-files do s3 raw, converter para parquet e gravá-los no s3 staging.

Lambda 2: az-latam-brazil-auxiliares-im-parquet-to-redshift.
Etapa responsável por carregar os dados do S3 staging e gravá-los no Redshift DW (DS)

Lambda 3: az-latam-brazil-auxiliares-im-redshift-to-sqlserver
Etapa responsável por chamar as stored procedures (SP) armazenada no SQL Server. Cada SP lê os dados no Redshift DW (DS) e persiste no SQL Server.
