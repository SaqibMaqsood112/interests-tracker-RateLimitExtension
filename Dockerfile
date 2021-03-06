FROM public.ecr.aws/lambda/python:3.8

RUN pip install --no-cache-dir sqlalchemy psycopg2-binary boto3 pyyaml

COPY app.py config.yaml ./

CMD ["app.lambda_handler"]

