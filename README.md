Before attempting to run any analysis scripts or models `data/Stocks_DLB.zip` needs to be unzipped into the data folder.  The file `data/Stocks_DLB.txt` should be present before any script execution.

After extracting the Stocks file, `exploration.Rmd` builds the data set from the multiple sources.

`modeling.Rmd` uses the unified data set to draw make estiamtes relevant to class action litigation.

`input_data.R` contains all the functions used to create the data set.  The first build took 30 minutes on a MacBook Pro.

`resultDataSet.csv` is an output of the completed and cleaned data for exploratory analysis.

`dataForAnalysis.csv` is the resultDataSet with logarithmic and relative transormations to create normality for modeling and hypothesis testing.