FROM python:3

RUN pip install pandas
RUN pip install numpy

RUN mkdir data
RUN mkdir output

COPY project_737491_d.py /project_737491_d.py
CMD ["python","/project_737491_d.py"]
ENTRYPOINT ["python","/project_737491_d.py"]
