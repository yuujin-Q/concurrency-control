# Mekanisme Concurrency Control dan Recovery

Tugas Besar IF3140 Manajemen Basis Data

## Deskripsi Tugas

Berikut adalah daftar task yang dilakukan pada tugas ini

- Eksplorasi Recovery
- Eksplorasi Transaction Isolation
- Implementasi Concurrency Control Protocol (2PL, OCC, MVCC)

Spesifikasi lengkap dapat dilihat pada folder [docs](docs/Spesifikasi%20Tugas%20Besar%20IF3140.pdf).

## Hasil Pengerjaan

### Laporan

Dokumen hasil pengerjaan dapat diakses pada folder [docs](docs/IF3140_K02_G10.pdf). Hasil eksplorasi recovery dan transaction isolation tertulis pada dokumen laporan.

### Cara Menjalankan Program (Implementasi Concurrency Control Protocol)

#### Pengaturan

Sebelum melakukan testing, sesuaikan jumlah CPU pada [txn_processor_text.cc ](txn/txn_processor_test.cc) baris 152 dengan jumlah Core CPU yang dimiliki pada komputer Anda.

#### Eksekusi

Jalankan command

```
make test
```

Untuk membersihkan hasil compile

```
make clean
```

## Contributors

Made with love by

| Nama                     | NIM      | Task                                                  |
| ------------------------ | -------- | ----------------------------------------------------- |
| Rachel Gabriela Chen     | 13521044 | Implementasi Concurrency Control Protocol (2PL)       |
| Eugene Yap Jin Quan      | 13521074 | Implementasi Concurrency Control Protocol (OCC, MVCC) |
| Fazel Ginanda            | 13521098 | Eksplorasi Recovery                                   |
| Muhammad Zaydan Athallah | 13521104 | Eksplorasi Transaction Isolation                      |
