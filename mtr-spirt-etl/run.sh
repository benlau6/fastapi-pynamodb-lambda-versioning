# update glue job
aws cloudformation deploy --template-file mtr-spirt-etl/template.yml --stack-name mtr-hk-dev-spirt-etl --capabilities CAPABILITY_IAM
# upload lib
cd mtr-spirt-etl/mtr_oeds-main
pip wheel .
cd ../../
aws s3 cp mtr-spirt-etl/mtr_oeds-main/mtr_oeds-0.1-py3-none-any.whl s3://mtr-hk-code-bucket/Spirt/dev/glue_job/lib/
# upload config
aws s3 cp mtr-spirt-etl/spirt_config.cfg s3://mtr-hk-code-bucket/Spirt/dev/glue_job/config/spirt_config.cfg
# upload glue script
aws s3 cp mtr-spirt-etl/mtr-hk-dev-spirt-etl.py s3://mtr-hk-code-bucket/Spirt/dev/glue_job/
# start glue job
aws glue start-job-run --job-name mtr-hk-dev-spirt-etl
# get status
aws glue get-job-runs --job-name mtr-hk-dev-spirt-etl


curl -X GET -H 