# Rocket's interceptor module

The interceptor module is responsible for initializing the XRPL validator node network and intercepting
all the messages sent between the nodes. This module is run as a subprocess of the controller, it is not a standalone 
application.

## Quickstart

### Prerequisites

- [rust and cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html)
- [protoc](https://github.com/hyperium/tonic?tab=readme-ov-file#dependencies)
- OpenSSL (read further for install guide)
- Docker (for Windows and macOS: make sure Docker Engine is active by launching Docker Desktop)

### OpenSSL

To install OpenSSL, see the official installation guide at https://github.com/openssl/openssl.
Although this is the official guide, there are alternatives that may be easier for you.
We recommend Windows users to use WSL with a Debian based distro as it is easier to install OpenSSL on it.

**Windows**

Note that this is for a 64 bit machine running on the x86_64 architecture. For other architectures
this may be a bit different.
For Windows, it is possible to download .exe/.msi installers via https://slproweb.com/products/Win32OpenSSL.html.
After installation, you should set two environment variables:

* OPENSSL_DIR = path_to\OpenSSL-Win64
* OPENSSL_LIB_DIR = path_to\OpenSSL-Win64\lib\VC\x64\MTd

Important Note: https://slproweb.com/products/Win32OpenSSL.html is a third-party website. This product has not been 
evaluated or tested by the OpenSSL project. While we have used their website without problem, we cannot guarantee
the same for you. 

**Linux**

For Debian based distro's, it is possible to use 'apt'.

```bash
sudo apt install openssl libssl-dev
```

**MacOS**

For MacOS, it is possible to use HomeBrew.

```bash
brew install openssl
```

### Building the executable

**Linux/macOS**

```bash
./build.sh
```

**Windows**

```powershell
.\build.bat
```

After executing the build script, the executable file should now be available in the root of this repository.

## Useful resources

- If you want to contribute read: [CONTRIBUTING.md](CONTRIBUTING.md)
- If you want to run the tests read: [TESTING.md](TESTING.md)
