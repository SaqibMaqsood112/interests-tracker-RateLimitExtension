FROM public.ecr.aws/lambda/python:3.8

RUN pip install --no-cache-dir sqlalchemy psycopg2-binary boto3

COPY app.py ./

CMD ["app.lambda_handler"]

