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

1. In your Python environment, install Python pacakges:

   ```python
   pip install rpy2
   ```

1. Run the conversion script:

   ```bash
   python odf_to_json.py test_files
   ```
