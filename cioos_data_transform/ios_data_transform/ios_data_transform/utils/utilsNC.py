import netCDF4 as nc
import gsw
import numpy as np


def combine_NCvariables(filename):
    
    #Load NC Dataset and get Variable list
    dset = nc.Dataset(filename,'r+')
    varList = dset.variables.keys() 

    #Variable naming convetion used
    varNameConv = 'BODC'

    #Create functions to be use internally
    #Function to detect if variable is available
    def hasVariable(inputStr,listString):
        result = any(x in inputStr for x in listString)
        return result

    #Function to replace Nan or flagged values by value from a second vector
    def fillNaN(x,y):
        x[np.isnan(x)] =y[np.isnan(x)]
        if np.size(x[:].mask)>1:
            x[x[:].mask] = y[x[:].mask]
        return x

    #Function to create a new variable in a NetCDF4 environement with similar type and dimensions than given variables.
    def createNewEmptyVariable(dset,varName,similarVarList,long_name,standard_name,units):
        multipleVar = 0
        varList = dset.variables.keys()
        
        #Make sure that all variable to merge are consistent if there's multiple in one dataset
        for x in similarVarList:
            #Compare similar varaibles to make sure dimensions and shape are similar
            if (multipleVar and hasVariable(x,varList)):
                if tempVar.dimensions != dset[x].dimensions:
                    print('dimensions are different between variables')
                if tempVar.shape != dset[x].shape:
                    print('shape is different between variables')
            
            # Find matching variable and create temporary variable based on the first match
            if (hasVariable(x,varList) and multipleVar!=1):
                tempVar = dset[x]
                multipleVar=1
                
        #Create the new variable with NaN values
        if multipleVar:
            varDims = tempVar.dimensions
            varType = tempVar.dtype
            
            if not hasVariable(varName,varList):
                dset.createVariable(varName,varType,varDims)
            
            dset[varName].long_name = long_name
            dset[varName].standard_name = standard_name
            dset[varName].units = units
        
        return dset

    #Combine Temperature Variables
    combine_var_names = ['TEMPS901','TEMP902','TEMPS601','TEMP602','TEMPRTN1','TEMPRTN2','TEMPST01','TEMPST02']

    if hasVariable(combine_var_names,varList):
        #Create New Variable          
        newVar = 'sea_water_temperature'
        createNewEmptyVariable(dset,newVar,combine_var_names,'Sea Water Temperature','sea_water_temperature','degC')
        var = dset[newVar]

        if varNameConv=='BODC': #If use BODC convention for variable names
            #Data already in ITS-90 Convention
            if hasVariable('TEMPS901',varList):
                fillNaN(var,np.array(dset.variables['TEMPS901']))
            if hasVariable('TEMPS902',varList):
                fillNaN(var,np.array(dset.variables['TEMPS902']))   
            
            #Convert IPTS-68 to ITS-90
            if hasVariable('TEMPS601',varList): #Convert Primary Temperature Sensor Data from IPTS-68 to ITS-90
                fillNaN(var,gsw.t90_from_t68(dset.variables['TEMPS601'][:]))
            if hasVariable('TEMPS602',varList): #Convert Seconday Temperature Sensor Data from IPTS-68 to ITS-90
                fillNaN(var,gsw.t90_from_t68(dset.variables['TEMPS602'][:]))
            
            #Older Standard which needs to be defined for now
            #if hasVariable('TEMPRTN',varList):
            #     fillNaN(var,np.array(dset.variables['TEMPRTN']))  
            #if hasVariable('TEMPST1',varList):
            #     fillNaN(var,np.array(dset.variables['TEMPST1']))  
            #if hasVariable('TEMPST2',varList):
            #     fillNaN(var,np.array(dset.variables['TEMPST2'])) 
    
    #Combine Salinity (sea_water_practical_salinity)
    combine_var_names = ['PSALST01','PSALST02','SSALST01','SSALST02','PSALBST01','PSALBST02','PSALBST1','PSALBST2','ODSDM021']

    if hasVariable(combine_var_names,varList):
        #Create New Variable        
        newVar = 'sea_water_practical_salinity'
        createNewEmptyVariable(dset,newVar,combine_var_names,'Sea Water Practical Salinity','sea_water_practical_salinity','')
        var = dset[newVar]

        if varNameConv=='BODC': #If use BODC convention for variable names
            #Data already in Practical Salinity unit
            if hasVariable('PSALST01',varList):
                fillNaN(var,np.array(dset.variables['PSALST01']))
            if hasVariable('PSALST02',varList):
                fillNaN(var,np.array(dset.variables['PSALST02']))   
            if hasVariable('PSALBST01',varList):
                fillNaN(var,np.array(dset.variables['PSALBST01']))
            if hasVariable('PSALBST02',varList):
                fillNaN(var,np.array(dset.variables['PSALBST02']))   
            if hasVariable('PSALBST1',varList):
                fillNaN(var,np.array(dset.variables['PSALBST1']))
            if hasVariable('PSALBST2',varList):
                fillNaN(var,np.array(dset.variables['PSALBST2']))
                
            #Data with Salinity in PPT convert to Pratical Salinity
            if hasVariable('SSALST01',varList): #Convert Primary Salinity Data from IPTS-68 to ITS-90
                fillNaN(var,gsw.SP_from_SK(dset.variables['SSALST01'][:]))
            if hasVariable('SSALST02',varList): #Convert Seconday Salinity Data from IPTS-68 to ITS-90
                fillNaN(var,gsw.SP_from_SK(dset.variables['SSALST02'][:]))
            if hasVariable('ODSDM021',varList): #Convert Seconday Salinity Data from IPTS-68 to ITS-90
                fillNaN(var,gsw.SP_from_SK(dset.variables['ODSDM021'][:]))  

    #Combine Depth (depth)
    combine_var_names = ['depth','PRESPR01','PRESPR02']

    if hasVariable(combine_var_names,varList):
        #Create New Variable            
        newVar = 'depth'
        createNewEmptyVariable(dset,newVar,combine_var_names,'Depth in meters','depth_below_sea_level_in_meters','m')
        var = dset[newVar]

        if varNameConv=='BODC':    
            #Data already in Depth (m)
            if hasVariable('depth',varList):
                fillNaN(var,np.array(dset.variables['depth']))
                
            #Convert Pressure to Pressure with TEOS-10 z_from_p tool 
            if (hasVariable('PRESPR01',varList) and hasVariable('latitude',varList)) : #Convert Primary Pressure Data from dbar to m
                fillNaN(var,gsw.z_from_p(dset.variables['PRESPR01'][:],dset.variables['latitude'][:]))
            if (hasVariable('PRESPR02',varList) and hasVariable('latitude',varList)) : #Convert Secondary Pressure Data from dbar to m
                fillNaN(var,gsw.z_from_p(dset.variables['PRESPR02'][:],dset.variables['latitude'][:]))

    
    #Combine pressure (sea_water_pressure)
    combine_var_names = ['PRESPR01','PRESPR02','depth']

    if hasVariable(combine_var_names,varList):
        #Create New Variable           
        newVar = 'sea_water_pressure'
        createNewEmptyVariable(dset,newVar,combine_var_names,'Sea Water Pressure in decibar','sea_water_pressure','dbar')
        var = dset[newVar]

        if varNameConv=='BODC': #If use BODC convention for variable names    
            #Data already in Sea Pressure (dBar)
            if hasVariable('PRESPR01',varList):
                fillNaN(var,np.array(dset.variables['PRESPR01']))
            if hasVariable('PRESPR02',varList):
                fillNaN(var,np.array(dset.variables['PRESPR02']))
                
            #Convert Depth to Pressure with TEOS-10 p_from_z tool 
            if (hasVariable('depth',varList) and hasVariable('latitude',varList)) : #Convert Primary Pressure Data from dbar to m
                fillNaN(var,gsw.p_from_z(-dset.variables['depth'][:],dset.variables['latitude'][:]))
                    
    #Save to NetCDF File
    dset.close()
