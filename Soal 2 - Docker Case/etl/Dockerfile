# Gunakan base image Python 3.11
FROM python:3.11.4

# Atur working directory di dalam container
WORKDIR /app

# Menyalin kode aplikasi Anda ke dalam container
COPY etl.py requirements.txt ./

# Menginstal dependencies (jika ada)
RUN pip install --no-cache-dir -r requirements.txt

# Menjalankan skrip etl.py saat container dijalankan
CMD ["python", "etl.py"]
