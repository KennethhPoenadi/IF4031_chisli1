# Temuan: run wipe-node pertama (BUKAN bukti lolos)

Hasil tes: GAGAL (Tester Agent tidak menyatakan selesai; node Mac tidak pulih dalam batas waktu 25 menit).

Yang terukur sah pada run ini:
- Selama node worker (Mac) mati sekitar 25 menit: 0 permintaan melanggar (read 1527, checksum 1526, write 2260, semuanya 200),
  partial=False sepanjang waktu.
- Event BMKG hilang: 0 dari 30.957 setelah node dihapus.

Penyebab kegagalan (lingkungan, bukan sistem): skenario wipe-node (wipe_and_shutdown) meninggalkan catatan run `status: go` di
ConfigMap tester-agent/tester-run (salinan: tester-run.yaml). Tiap kali pod Tester Agent di node itu naik lagi, run yang belum
ditutup dieksekusi ulang: data node dihapus lagi dan VM dimatikan lagi. Catatan itu dihapus manual atas perintah pemilik cluster.
Penghapusan data di tengah jalan juga merusak penyimpanan containerd (snapshot "already exists"); node baru sehat setelah
/var/lib/rancher/k3s/agent/containerd dibuang dan folder volume store-1 dibuat ulang.
