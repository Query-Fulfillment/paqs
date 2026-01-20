FROM pcornetqf/qf-r-base:2.0.1

RUN mkdir /app


COPY query /app/query
COPY tools /app/tools

WORKDIR /app


RUN chmod u+x tools/handler.sh

CMD ["tools/handler.sh"]