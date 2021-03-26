# ODF Transform

## Converting ODF to JSON

This module uses the R OCE package from within Python. You will need to install R and libraries.

1. You may need to use R version 3.6.2

  <https://cran.r-project.org/bin/windows/base/old/3.6.2/>


1. These commands should be run in R:

   ```R
   install.packages("devtools", dependencies = TRUE)
   library(devtools)
   install_github("dankelley/oce", ref="develop")
   install.packages("RJSONIO", dependencies = TRUE)
   library(RJSONIO)
   ```

  If some libraries cannot be installed using system R, the following command can be used to
  install many of them:

  ```console
  sudo apt install libfontconfig1-dev libssl-dev libxml2-dev libcurl4-openssl-dev libudunits2-dev libgdal-dev libproj-dev
  ```

1. In your Python environment, install Python packages:

   ```python
   pip install rpy2
   ```

1. Run the conversion script:

   ```bash
   python odf_to_json.py test_files
   ```
