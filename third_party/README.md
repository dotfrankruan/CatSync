# Third-party components

CatSync itself is licensed under AGPL-3.0-only (see [LICENSE](../LICENSE)).
The binary links the following Go modules, each under its own BSD-style
license; the full license texts live in [licenses/](licenses/).

| Module | Version | License | Used for |
| --- | --- | --- | --- |
| github.com/pkg/sftp | v1.13.10 | BSD-2-Clause ([text](licenses/pkg-sftp.txt)) | native multistream large-file backend's SFTP client |
| golang.org/x/crypto | v0.41.0 | BSD-3-Clause ([text](licenses/golang-x-crypto.txt)) | pkg/sftp's SSH wire crypto |
| golang.org/x/sys | v0.35.0 | BSD-3-Clause ([text](licenses/golang-x-sys.txt)) | syscall support of the above |
| github.com/kr/fs | v0.1.0 | BSD-3-Clause ([text](licenses/kr-fs.txt)) | pkg/sftp's filesystem helpers |

Nothing else is linked: no fpsync, fpart, or mscp binaries are needed,
bundled, or executed.
