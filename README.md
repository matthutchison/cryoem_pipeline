# CryoEM Pipeline

This project integrates data management functions with a CryoEM quality control and preprocessing package.

## Getting Started

### Prequisittes

Python 3.5 or higher is required. The application uses features from the asyncio module that were not introduced until 3.5. It has not been tested with 3.7, but should work.

The globus command line tools must be installed for the globus data transfer functions to work. These will not impede the rest of the operations, but you will get logging errors without them. If just compressing to a local drive and handling web-transfer separately.

By default, lbzip2 and IMOD are also required. The functions that use them could be patched out pretty reasonably (replacing lbzip2 with another parallel compression application or replacing the few IMOD commands that are used with something else).

The wrapper assumes that it is supporting an existing Scipion installation.  In the future, this will be a more flexible function but for now is a fairly rigid requirement. The assumptions are that

- Scipion is installed and running
- The html status monitoring page is in use

This application also requires the ```transitions``` python module (available on pypi), which in turn requires ```six```. Do the following after cloning to install the required module with the standard
```shell
pip install -r requirements.txt
```
**Note:** It is generally not advised to globally install python packages. One approach that works well is to pull the package down and place it near or in the pipeline install, then write a wrapper script for pipeline that sets PYTHONPATH to the location of the transitions package. Virtualenvs may also be an effective approach.

### Installation

Setup is currently a very manual process, requiring multiple hardcoded options to be changed to fit the target system. All of this is planned to move to a config file that make much more sense; once that happens this section will be fleshed out more.  Some useful tips are listed here.

- Clone the repository to the target machine using
```shell
git clone https://github.com/abcsFrederick/cryoem_pipeline.git
```
- Add appropriate configuration files to the cryoem_pipeline/config/system/ and cryoem_pipeline/config/user/ directories. README files in each will give you more information about what to put there.  Not all values need to be set in configuration files; the application will prompt for expected values that it does not find prior to fully initializing the application.
- Put the working directory on a with high throughput and enough space to have 20-30 files in-process at any given time. We use at least 100GB and SATA or NVME SSD's (or in-memory drives) as our scratch space. Do not use a networked drive for "working_directory"
- The storage directory must have enough space to hold the entire compressed dataset; this directory is the final resting place of data pre-globus-transfer and in the interest of safety is not cleaned up by the pipeline.