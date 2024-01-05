# Menggunakan Python 3.11.4 sebagai base image
FROM python:3.11.4

# Mengatur direktori kerja di dalam container
WORKDIR /app

# Menyalin kode dan requirements.txt ke dalam container
COPY api.py requirements.txt ./

# Menginstall library yang diperlukan
RUN pip install --no-cache-dir -r requirements.txt

# Menjalankan aplikasi saat container dijalankan
CMD ["python", "api.py"]