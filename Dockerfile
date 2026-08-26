ARG BASE
FROM ${BASE}

# /opt/tinfoil rather than dist-packages: the bases span several Python
# versions, so the site-packages path is not the same in all of them.
COPY tinfoil_usage.py /opt/tinfoil/tinfoil_usage.py
ENV PYTHONPATH=/opt/tinfoil

RUN python3 -B -c "import tinfoil_usage; print('usage metering ready:', tinfoil_usage.TRAILER_SUPPORT)"
