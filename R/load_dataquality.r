

exclude_query <- "and COLUMN_NAME not like 'SYS_%'
and COLUMN_NAME not like 'PSP_%'
and COLUMN_NAME not like 'I_SYS_%'
and COLUMN_NAME not like 'C_SYS_%'
and COLUMN_NAME not like 'DV_SYS_MERGE'
and COLUMN_NAME not like '%_SYS_DOMAIN'"

exclude_cols_df <- c("ID", "number", "SYS",'PSP_','C_SYS_','DV_SYS_MERGE','_SYS_DOMAIN')


get_list_of_dfs <- function(DQ_table,connection){
  lst <- ''
  query_tbl <- sprintf("select distinct table from %s", DQ_table)
  tables_df <- dbGetQuery(connection, query_tbl)
  
  if(is.data.frame(tables_df) && nrow(tables_df) > 0){
    lst <- tables_df[['TABLE']]
  }
  #print(lst)
  return(lst)
}

find_duplicate_entry <- function(df, attr_to_consider,schema, table, mapping_table, connection){
  
  duplicate_count <- 0
  column_id <- ''
  
  query_column_id <- sprintf("select COLUMN_ID from %s WHERE SCHEMA_NAME = '%s' AND TABLE_NAME = '%s' 
                             and COLUMN_ID is not null",
                             dq_table_list, schema, table )
  
  column_id_df <- dbGetQuery(connection, query_column_id)
  
  
  if(is.data.frame(column_id_df) && nrow(column_id_df) > 0 ){
    
    column_id <- column_id_df[['COLUMN_ID']]
    
    df_duplicate <- df[which(duplicated(df[attr_to_consider]) | duplicated(df[attr_to_consider], fromLast = TRUE)),]
    if(nrow(df_duplicate) > 0){
      duplicate_count <- nrow(df_duplicate)
    }
  }
  return(duplicate_count)
  
}

is_first_entry <- function(DQ_table, table, connection){
  
  count <- 0
  query <- paste0(paste0(paste0("select count(*) from ", DQ_table),
                         " where \"TABLE\" = '", table), "'")
  
  
  #print(query)
  count_records <- dbGetQuery(connection,query)
  
  count <- count_records[1,1]
  return(count)
}

get_latest_entry <- function(DQ_table, table, connection){
  
  query <- sprintf("select * from %s where \"TABLE\" = '%s' 
                   AND TO_VARCHAR (TO_DATE(\"SYSTEM_DATE\"), 'YYYY-MM-dd')  = (select MAX(TO_VARCHAR (TO_DATE(\"SYSTEM_DATE\"), 'YYYY-MM-dd'))  
                   from %s where \"TABLE\" = '%s')
                   ",DQ_table,table,DQ_table,table)
  
  summary_prev_date <- dbGetQuery(connection, query)
  return(summary_prev_date)
}

get_DF_types <- function(df){
  
  datatypes_list <- sapply (df, class)
  df_datatypes <- data.frame(matrix(unlist(datatypes_list), nrow=length(datatypes_list), byrow=TRUE))
  colnames(df_datatypes)[1] <- "Data_Type"
  # View(df_datatypes)
  # break
  return(df_datatypes)
}

get_data_till_date <- function(table, columns_list, date, source_connection) {
  
  if(table == 'DATASCIENCE.CEO_METRIC'){
    
    #query <- sprintf("SELECT \"Closed\",\"WeightedPipeline\",\"TotalPipeline\",\"TotalOpenPipeline\",\"Renewals\" ,\"OpenRenewals\",\"MaturedPipeline\",\"UnqualifiedPipeline\" FROM \"DATASCIENCE\".\"CEO_METRIC_10052020\" where TO_VARCHAR (TO_DATE(\"ExtractDate\"), 'YYYY-MM-dd') <= '%s' order by \"ExtractDate\" ", date)
    
    raw_data <- dbGetQuery(source_connection, " SELECT ROUND(SUM(CASE WHEN \"ForecastStage\" = '8 - Closed Won (100%)' AND SUBSTRING(QUARTER(\"ExtractDate\"), 3) = \"CloseQuarter\" THEN \"USDNetNewACV\" END )) as \"Closed\"
                           , ROUND(SUM(CASE WHEN SUBSTRING(QUARTER(\"ExtractDate\"), 3) = \"CloseQuarter\" THEN \"USDWeightedNNACV\" END)) as \"WeightedPipeline\"
                           , ROUND(SUM(CASE WHEN SUBSTRING(QUARTER(\"ExtractDate\"), 3) = \"CloseQuarter\" THEN \"USDNetNewACV\" END)) as \"TotalPipeline\"
                           , ROUND(SUM(CASE WHEN \"ForecastStage\" NOT IN ('8 - Closed Won (100%)', '8 - Closed Lost (100%)') AND SUBSTRING(QUARTER(\"ExtractDate\"), 3) = \"CloseQuarter\" THEN \"USDNetNewACV\" END)) as \"TotalOpenPipeline\"
                           , ROUND(SUM(CASE WHEN SUBSTRING(QUARTER(\"ExtractDate\"), 3) = \"CloseQuarter\" THEN \"USDRenewalACV\" END)) as \"Renewals\"
                           , ROUND(SUM(CASE WHEN \"ForecastStage\" NOT IN ('8 - Closed Won (100%)', '8 - Closed Lost (100%)') THEN \"USDRenewalACV\" END)) as \"OpenRenewals\"
                           , ROUND(SUM(CASE WHEN \"ForecastStage\" IN ('6 - Validation Completed (70%)', '7 - Deal Imminent (90%)') THEN \"USDNetNewACV\" END)) as \"MaturedPipeline\"
                           , ROUND(SUM(CASE WHEN \"ForecastStage\" IN ('1 - Opportunity (1%)','2 - Discovery (5%)','3 - Business Issue (10%)') AND SUBSTRING(QUARTER(\"ExtractDate\"), 3) = \"CloseQuarter\" THEN \"USDNetNewACV\" END)) as \"UnqualifiedPipeline\"
                           FROM \"_SYS_BIC\".\"REAL-TIME-SALES.SELFSERVICE/RPT_SALES_OPPORTUNITYITEM_SNAPSHOT_SELFSERVE\"
                           ('PLACEHOLDER' = ('$$Frequency$$', 'Daily'), 'PLACEHOLDER' = ('$$Data$$', '1')) where  TO_VARCHAR (TO_DATE(\"ExtractDate\"), 'YYYY-MM-dd') > '2020-08-30' GROUP BY \"ExtractDate\" order BY \"ExtractDate\" ")
    
    #raw_data1 <- raw_data[raw_data$ExtractDate <= date,!names(raw_data) %in% 'ExtractDate']
    
  }
  else{
    query <- sprintf("select %s from %s  ", columns_list, table)
    #print(query)
    raw_data <- dbGetQuery(source_connection, query)
  }
  
  return(raw_data)
}

find_value_anomaly_df <- function(df, column, df_type ='', summary_prev_date){
  
  var_in_percentage <- 0
  cur_val <- round(as.numeric(lapply(df[column], mean, na.rm = TRUE)), 2)
  mean_val <- cur_val
  sd_val <- as.numeric(lapply(df[column], sd, na.rm = TRUE))
  
  mean_val <- ifelse(!is.nan(mean_val),mean_val,0)
  sd_val <- ifelse(!is.na(sd_val),sd_val,0)
  
  thres_u_mean <- ifelse(!is.na(round(mean_val + (3 * sd_val), 2)),round(mean_val + (3 * sd_val), 2), 0)
  thres_l_mean <- ifelse(!is.na(round(mean_val - (3 * sd_val), 2)),round(mean_val - (3 * sd_val), 2),0)
  
  if(is.data.frame(summary_prev_date) && nrow(summary_prev_date) > 1 && length(df_type) > 0 && df_type != 'BASE') {
    prev_value <- as.numeric(summary_prev_date[summary_prev_date$COLUMN == column,]['VALUE'])
    var_in_percentage <- ifelse(!is.nan(round(((cur_val - prev_value)/prev_value)*100,2)), round(((cur_val - prev_value)/prev_value)*100,2), 0)
  }
  
  
  # if(column == 'TotalVisits'){
  #   print(var)
  #   break
  # }
  return_val <- ifelse(var_in_percentage > 30, 1  ,0)
  
  result_list <- list(return_val, thres_u_mean, thres_l_mean)
  
  return(result_list)
  
}

find_value_anomaly <- function(df, column, anomaly_column, table_name, dq_table, anomaly_calculation_period, df_first_name ='') {
  
  jdbcConnection <- get_connection_ebq()
  return_val <- 0
  result_list <- list()
  thres_u_mean <- 0
  thres_l_mean <- 0
  
  cur_val <- ifelse(!is.na(df[nrow(df),column]),df[nrow(df),column],0)
  dq_latest_records <- df[-nrow(df),] 
  
  if(nrow(dq_latest_records) >= 29) {
    
    mean_val <- as.numeric(lapply(dq_latest_records[column], mean, na.rm = TRUE))
    sd_val <- as.numeric(lapply(dq_latest_records[column], sd, na.rm = TRUE))
    
    mean_val <- ifelse(!is.nan(mean_val),mean_val,0)
    sd_val <- ifelse(!is.na(sd_val),sd_val,0)
    
    thres_u_mean <- round(mean_val + (3 * sd_val), 2)
    thres_l_mean <- round(mean_val - (3 * sd_val), 2)
    
    print("Threshhold#######")
    if(cur_val > thres_u_mean || cur_val < thres_l_mean){
      return_val <- 1
    }
  }
  
  result_list <- list(return_val, thres_u_mean, thres_l_mean)
  
  return(result_list)
}

find_anomalies <- function(column, val ,varience_column, table_name, dq_table, anomaly_calculation_period, df_first_name ='') {
  
  jdbcConnection <- get_connection_ebq()
  return_val <- 0
  results_list <- list()
  lower_limit <- 0
  upper_limit <- 0
  print(paste0('=============>',df_first_name))
  
  if(trim(df_first_name) == ''){ print("here---155") 
    print(trim(df_first_name))
    query <- sprintf("select top %s %s from %s where COLUMN = '%s' AND TABLE = '%s' ORDER BY SYSTEM_DATE DESC;",
                     anomaly_calculation_period,varience_column, dq_table, column , table_name)
  }else{
    query <- sprintf("select top %s %s from %s where COLUMN = '%s' AND TABLE like '%%%s%%' ORDER BY SYSTEM_DATE DESC;",
                     anomaly_calculation_period,varience_column, dq_table, column , df_first_name)
  }
  
  #print(query)
  print('==========>166')
  dq_latest_records <- dbGetQuery(jdbcConnection, query)
  #View(dq_latest_records)
  #break
  
  
  if(nrow(dq_latest_records) >= anomaly_calculation_period) {
    data_std <- sd(dq_latest_records[[varience_column]])
    data_mean <- mean(dq_latest_records[[varience_column]])
    anomaly_cut_off <- data_std * 3
    print("######################")
    
    lower_limit  <- data_mean - anomaly_cut_off 
    upper_limit <- data_mean + anomaly_cut_off
    
    if(val > upper_limit || val < lower_limit){
      return_val <- 1
    }
  }
  results_list <- list(return_val,upper_limit,lower_limit)
  # if(column == 'EmailOpenIn180Days'){
  #   print(nrow(dq_latest_records))
  #   print(anomaly_calculation_period)
  #   print(val)
  #   print(results_list)
  #   print(upper_limit)
  #   print(lower_limit)
  #   print(anomaly_cut_off)
  #   break
  # }
  return(results_list)
}

find_anomalies_iqr <- function(column, val ,prev_val, column_for_anomaly, table_name, dq_table) {
  
  jdbcConnection <- get_connection_ebq()
  return_val <- 0
  results_list <- list()
  lower_limit <- 0
  upper_limit <- 0
  threshold_upper <- 0
  threshold_lower <- 0
  query <- sprintf("select top 5 %s from %s where COLUMN = '%s' AND TABLE = '%s' ORDER BY SYSTEM_DATE DESC;",column_for_anomaly, dq_table, column , table_name)
  
  dq_latest_records <- dbGetQuery(jdbcConnection, query)
  
  
  if(nrow(dq_latest_records) >= 5) {
    
    lowerq = quantile(dq_latest_records[[column_for_anomaly]], 0.25)#[2]
    upperq = quantile(dq_latest_records[[column_for_anomaly]],0.75)#[4]
    iqr = upperq - lowerq #Or use IQR(data)
    
    threshold_upper = (iqr * 1.5) + upperq
    threshold_lower = lowerq - (iqr * 1.5)
    
    if(val > threshold_upper || val < threshold_lower){
      return_val <- 1
    }
  }
  
  results_list <- list(return_val,threshold_upper,threshold_lower)
  return(results_list)
}


get_column_score <- function(new_df, row_count, count_variance, count_anomaly, anomaly_calculation_period, target_count_variance, duplicate_entry, is_not_numeric){
  
  target_count_score <- 0
  missing_anomaly_score <- 0
  count_anomaly_score <- 0
  unique_anomaly_score <- 0
  colshift_anomaly_score <- 0
  duplicate_score <- 0
  value_anomaly_score <- 0
  perc_accuracy_colshift <- 20
  perc_accuracy_other <- 40
  perc_accuracy_value <- 20
  percentage <- (100/3)/100
  duplicate_threshold <- 1
  
  missing_threshold <- 30
  unique_threshold <- 30
  count_threshold <- 30
  colshift_threshold <- 30
  value_anomaly_threshold <- 30
  
  df <- as.data.frame(FALSE,)
  
  if(nrow(new_df) > 0){
    
    if(row_count == 0){
      accuracy <- 0
      completeness <- 0
      uniqueness <- 0
      overall_score <- 0
    }
    else{
      #if(row_count > anomaly_calculation_period){
      
      ###table level scoring########
      count_anomaly_score <- table_level_score(count_anomaly, count_variance, count_threshold)
      duplicate_score <- table_level_score(duplicate_entry, '', duplicate_threshold)
      
      if(target_count_variance == 0){
        target_count_score <- 0
      }else if(between(target_count_variance,-0.10,0.10)){
        target_count_score <- 0.5
      }else{
        target_count_score <- 1
      }
      #############################
      
      ####Column level scoring######
      value_anomaly_score <- anomaly_score_in_percentage(new_df, 'VALUE_ANOMALY', 'VALUE_VARIANCE', value_anomaly_threshold, 1)
      missing_anomaly_score <- anomaly_score_in_percentage(new_df, 'MISSING_ANOMALY', 'MISSING_VARIANCE', missing_threshold, 1)
      unique_anomaly_score <- anomaly_score_in_percentage(new_df, 'UNIQUE_ANOMALY', 'UNIQUE_VARIANCE', unique_threshold, 1)
      colshift_anomaly_score <- anomaly_score_in_percentage(new_df, 'COLSHIFT_ANOMALY', 'COLSHIFT_ANOMALY', colshift_threshold, 0)
      ##############################
      print("===============Individual===========")
      print(missing_anomaly_score)
      print(count_anomaly_score)
      print(unique_anomaly_score)
      print(colshift_anomaly_score)
      print(duplicate_score)
      print(value_anomaly_score)
      print("===============Individual===========")
      #}
      
      if(is_not_numeric == 0){
        accuracy <- 100 - ((value_anomaly_score * perc_accuracy_value) + (count_anomaly_score * perc_accuracy_other) + (target_count_score * perc_accuracy_other))
        print(accuracy)
      }
      else{
        accuracy <- 100 - ((count_anomaly_score * perc_accuracy_other) + (colshift_anomaly_score * perc_accuracy_colshift) + (target_count_score * perc_accuracy_other))
      }
      print("=============Score##########")
      completeness <- 100 - (missing_anomaly_score * 100)
      uniqueness	<- 100 - ((unique_anomaly_score * 50) + (duplicate_score * 50))
      
      overall_score <- ((completeness * percentage) +
                          (accuracy * percentage) +
                          (uniqueness * percentage) )
      
      print(accuracy)
      print(completeness)
      print(uniqueness)
      print(overall_score)
      
      
    }
    
    df['COLUMN'] <- new_df[["COLUMN"]]
    df['ACCURACY'] <- accuracy
    df['COMPLETENESS'] <- completeness
    df['UNIQUENESS'] <- uniqueness
    df['TIMELINESS'] <- 0
    df['OVERALL_SCORE'] <- overall_score
    
    return(df)
  }
}
get_summary_col <- function(df, column, cat_attr, summary_prev_date, anomaly, table, dq_table, anomaly_calculation_period, df_first_name = '', df_type = ''){
  
  colshift_flag <- ''
  colshift_val <- ''
  count_variance <- 0
  colshift_variance <- 0
  missing_variance <- 0
  unique_variance <- 0
  row_count <- 0
  unique_count <- 0
  missing_coun <- 0
  count_anomaly <- 0
  missing_anomaly <- 0
  unique_anomaly <- 0
  colshift_anomaly <- 0
  value_anomaly <- 0
  value_variance <- 0
  value <- 0
  upper_threshold <- 0
  lower_threshold <- 0
  missing_upper_threshold <- 0
  missing_lower_threshold <- 0
  unique_upper_threshold <- 0
  unique_lower_threshold <- 0
  new_df <- as.data.frame(FALSE,)
  res_value_list <- c('')
  
  
  row_count <- as.numeric(nrow(df))
  missing_count <- as.numeric(sum(is.na(df[column])))
  
  
  if(length(df_type) > 0 & df_type != 'BASE'){
    colshift_data <- get_colshift_df(df, column, missing_count , summary_prev_date[summary_prev_date$COLUMN == column,]['DATA_TYPES'] )
    colshift_flag <- colshift_data[['X1']]
    colshift_val <- colshift_data[['X2']]
  }
  else{
    colshift_data <- get_colshift_flag(df, column, missing_count)
    colshift_flag <- colshift_data[['X1']]
    colshift_val <- colshift_data[['X2']]
  }
  #break
  
  if(cat_attr == 1){
    unique_count <- as.numeric(length(unique(df[[column]])))
  }else{ 
    value <- ifelse(length(df_type) == 0, ifelse(!is.na(df[nrow(df),column]),df[nrow(df), column],0), as.numeric(lapply(df[column], mean, na.rm = TRUE)))
    
  }
  
  if(cat_attr == 0 && nrow(df) >= 29 && length(df_type) == 0){
    res_value_list <- find_value_anomaly(df, column, 'VALUE_ANOMALY', table, dq_table, anomaly_calculation_period, df_first_name)
    
  }else if(cat_attr == 0 && length(df_type) > 0){
    res_value_list <- find_value_anomaly_df(df, column, df_type, summary_prev_date)
    
  }
  if(length(res_value_list) > 1){
    value_anomaly <- res_value_list[[1]]
    upper_threshold <- res_value_list[[2]]
    lower_threshold <- res_value_list[[3]]
  }
  if(is.data.frame(summary_prev_date) && nrow(summary_prev_date) > 1){
    
    print("here=====================357")
    if(length(summary_prev_date$COLUMN[summary_prev_date$COLUMN == column]) > 0 ){
      
      prev_missing_count <- as.numeric(summary_prev_date$MISSING[(summary_prev_date$COLUMN == column)])
      prev_unique_count <- as.numeric(summary_prev_date$UNIQUE[(summary_prev_date$COLUMN == column)])
      prev_colshift_flag <- summary_prev_date$COLSHIFT[(summary_prev_date$COLUMN == column)]
      prev_colshift_anomaly_flag <- summary_prev_date$COLSHIFT_ANOMALY[(summary_prev_date$COLUMN == column)]
      prev_value <- as.numeric(summary_prev_date$VALUE[(summary_prev_date$COLUMN == column)])
      
      print(paste0("prev_colshift_flag --->",prev_colshift_flag))
      print(paste0("colshift_flag --->",colshift_flag))
      
      if(prev_colshift_flag == 'N' & colshift_flag == 'Y'){
        colshift_anomaly <- 1
      }else if(prev_colshift_flag == 'Y' & colshift_flag == 'Y' & prev_colshift_anomaly_flag == 1){
        colshift_anomaly <- 1
      }else{
        colshift_anomaly <- 0
      }
      
      if(prev_missing_count != 0){
        missing_variance <- round(((missing_count - prev_missing_count)/prev_missing_count)*100,2)
      }else{
        missing_variance <- round((missing_count)*100,2)
      }
      
      #supressing missing variance if negative
      if(missing_count < prev_missing_count | missing_variance <= 0 ){
        missing_variance <- 0
      }
      
      if(cat_attr == 0 && !is.na(prev_value) && prev_value != 0){ print('----------460')
        value_variance <- abs(round(((value - prev_value)/prev_value)*100,2))
      }else{
        value_variance <- abs(round((value)*100,2))
      }
      
      ##additional cond for DF for getting insights
      value_anomaly <- ifelse(value_anomaly == 0 && value_variance != 0 && length(df_type) > 0, 2, value_anomaly)
      
      print("here=====================394")
      if(cat_attr == 1){
        if(prev_unique_count != 0){
          unique_variance <- round(((unique_count - prev_unique_count)/prev_unique_count)*100,2)
        }else{
          unique_variance <- round((unique_count)*100,2)
        }
        print(paste0("unique_variance --->",unique_variance))
      }
      
      
      print("###Anomaly calculation###")
      if(anomaly == 1){
        missing_anomaly_res <- find_anomalies(column, missing_variance , 'MISSING_VARIANCE', table, dq_table, anomaly_calculation_period, df_first_name)
        
        if(length(missing_anomaly_res) > 0){
          missing_anomaly <- missing_anomaly_res[[1]]
          missing_upper_threshold <- missing_anomaly_res[[2]]
          missing_lower_threshold <- missing_anomaly_res[[3]]
        }
        if(missing_variance != 0 && length(df_type) > 0){
          missing_anomaly <- 2
        }
        
        
        #code to deflag the missing anomaly if missing count is less than 10% of row count 
        # if(round((missing_count/row_count)*100) < 10 ){
        #   missing_anomaly <- 0
        # }
        
        if(unique_variance == 0 ){
          unique_anomaly <- 0
        }
        else if(unique_variance != 0 && length(df_type) > 0){
          unique_anomaly <- 2
        }
        else{
          unique_anomaly_res <- find_anomalies_iqr(column, unique_count, prev_unique_count , 'UNIQUE', table, dq_table)
          
          if(length(unique_anomaly_res) > 0){
            unique_anomaly <- unique_anomaly_res[[1]]
            unique_upper_threshold <- unique_anomaly_res[[2]]
            unique_lower_threshold <- unique_anomaly_res[[3]]
          }
          
        }
        
      }
      print("### End ###")
    }
  }
  # 
  # if(column == 'DaysSinceLastMeeting'){
  #   print(cat_attr)
  #   print(value)
  #   print(value_anomaly)
  #   print(value_variance)
  #   break
  # }
  missing_variance <- ifelse(missing_variance > 100, 100, missing_variance)
  value_variance <- ifelse(value_variance > 100, 100, value_variance)
  unique_variance <- ifelse(unique_variance > 100, 100, unique_variance)
  
  new_df['SYSTEM_DATE'] <- format(Sys.time(), "%Y-%m-%d %X")
  new_df['COLUMN'] <- column
  new_df['MISSING'] <- missing_count
  new_df['MISSING_VARIANCE'] <- abs(missing_variance)
  new_df['MISSING_ANOMALY'] <- missing_anomaly
  new_df['UNIQUE'] <- unique_count
  new_df['UNIQUE_VARIANCE'] <- abs(unique_variance)
  new_df['UNIQUE_ANOMALY'] <- unique_anomaly
  new_df['COLSHIFT'] <- colshift_flag
  new_df['COLSHIFT_VALS'] <- colshift_val
  new_df['COLSHIFT_ANOMALY'] <- colshift_anomaly
  new_df['VALUE'] <- value
  new_df['VALUE_VARIANCE'] <- abs(value_variance)
  new_df['VALUE_ANOMALY'] <- value_anomaly
  new_df['VALUE_UPPER_THRESHOLD'] <- upper_threshold
  new_df['VALUE_LOWER_THRESHOLD'] <- lower_threshold
  new_df['UNIQUE_UPPER_THRESHOLD'] <- unique_upper_threshold
  new_df['UNIQUE_LOWER_THRESHOLD'] <- unique_lower_threshold
  new_df['MISSING_UPPER_THRESHOLD'] <- missing_upper_threshold
  new_df['MISSING_LOWER_THRESHOLD'] <- missing_lower_threshold
  
  return(new_df)
}
table_level_score <- function (count_anomaly, count_variance, threshold){
  
  if(count_variance != ''){
    if(count_anomaly == 1 && count_variance > threshold){
      score <- 1
    }else if(count_anomaly == 1){
      score <- 0.5
    }else{
      score <- 0
    }
  }
  else{
    #duplicate
    if(count_anomaly > threshold){
      score <- 1
    }else if(count_anomaly == threshold){
      score <- 0.5
    }else{
      score <- 0
    }
  }
  
  return(score)
}

anomaly_score_in_percentage <- function(summery_data, var_anomaly, var_variance, threshold, column_level){
  
  num_rows <- nrow(summery_data)
  variance_above_threshold <- 0
  print('============')
  #View(summery_data)
  print(summery_data[var_anomaly])
  #break
  if(column_level == 1){
    
    variance_above_threshold <- nrow(summery_data[summery_data[var_anomaly] == 1 & summery_data[var_variance] >= threshold, ])
    count_anomaly_col <- nrow(summery_data[summery_data[var_anomaly] == 1,])
    count_anomaly_col_per <- (count_anomaly_col/num_rows) * 100
    
    if(var_anomaly == 'MISSING_ANOMALY'){
      # print(threshold)
      # #
      # print(variance_above_threshold)
    }
    if(summery_data[var_anomaly] == 1 & summery_data[var_variance] >= threshold){ 
      anomaly_score_val <- 1
    }else if(summery_data[var_anomaly] == 1){
      anomaly_score_val <- 0.5
    }else if(count_anomaly_col_per > 0 & count_anomaly_col_per < threshold){
      anomaly_score_val <- 0.5
    }else{
      anomaly_score_val <- 0
    }
  }
  else{
    if(summery_data[['COLSHIFT_ANOMALY']] == 1){
      anomaly_score_val <- 1
    }else{
      anomaly_score_val <- 0
    }
    
  }
  
  return(anomaly_score_val)
}

get_timeliness_score <- function(table, connection){
  
  timeliness_score <- -1
  query <- paste0(paste0("select flag from DATASCIENCE.ANALYTICS_DATAQUALITY_TIMELINESS  where \"TABLE\" = '",table),"'")
  #print(query)
  timeliness_df <- dbGetQuery(connection, query)
  
  if(is.data.frame(timeliness_df) && nrow(timeliness_df) > 0 ){
    timeliness_flag <- timeliness_df[['FLAG']]
    if(timeliness_flag == 1){
      timeliness_score <- 1
    }else{
      timeliness_score <- 0
    }
  }
  
  return(timeliness_score)
}


get_colshift_df <- function(df, column , count_na, col_base_dtype) {
  
  new_df <- data.frame(matrix(ncol = 2, nrow = 0))
  
  total_count <- nrow(df)
  colshift_vals <- list()
  colshift_flag <- 'N'
  colshift_val <- ''
  print("_______________________....>>>>>>")
  print(dim(col_base_dtype))
  print(col_base_dtype$DATA_TYPES)
  print(class(df[[column]]))
  
  # print(str_replace(class(df[[column]]), '[\"]', ''))
  # 
  if(col_base_dtype$DATA_TYPES != class(df[[column]])){
    print('herer==========')
    print(column)
    
    if(col_base_dtype$DATA_TYPES == 'integer'){
      print("line====================613")
      
      if(!is.integer(df[[column]]) && (countNotNumeric(df[[column]]) == 0)){
        colshift_flag <- 'Y'
        colshift_vals[['val']] <- df[grepl("[0-9]+([.][0-9]+)", df[[column]]),][[column]]
        
        if(length(unique(colshift_vals[['val']])) > 10){
          colshift_val <- paste0(noquote(paste0(head(unique(unlist(colshift_vals, use.names = FALSE)),10), collapse = ',')),' and more')
        }
        else if(length(unique(colshift_vals[['val']])) > 0){
          colshift_val <- noquote(paste0(unique(unlist(colshift_vals, use.names = FALSE)), collapse = ','))
        }
      }
      else if((countNotNumeric(df[[column]]) == 0) | (countNotNumeric(df[[column]]) == total_count)){
        colshift_flag <- 'N'
      }else if(countNotNumeric(df[[column]]) > count_na){
        colshift_flag <- 'Y'
        colshift_vals[['val']] <- df[grepl("[A-Za-z]", df[[column]]),][[column]]
        
        if(length(unique(colshift_vals[['val']])) > 10){
          colshift_val <- paste0(noquote(paste0(head(unique(unlist(colshift_vals, use.names = FALSE)),10), collapse = ',')),' and more')
        }
        else if(length(unique(colshift_vals[['val']])) > 0){
          colshift_val <- noquote(paste0(unique(unlist(colshift_vals, use.names = FALSE)), collapse = ','))
        }
        
      }else{
        colshift_flag <- 'N'
      }
    }
    else{
      print("line====================633")
      if((countNotNumeric(df[[column]]) == 0) | (countNotNumeric(df[[column]]) == total_count)){
        colshift_flag <- 'N'
      }else if(countNotNumeric(df[[column]]) > count_na){
        colshift_flag <- 'Y'
        colshift_vals[['val']] <- df[!grepl("[A-Za-z]", df[[column]]),][[column]]
        
        if(length(colshift_vals[['val']]) > 10){
          colshift_val <- paste0(noquote(paste0(head(unique(unlist(colshift_vals, use.names = FALSE)),10), collapse = ',')),' and more')
        }
        else if(length(colshift_vals[['val']]) > 0){
          colshift_val <- noquote(paste0(unique(unlist(colshift_vals, use.names = FALSE)), collapse = ','))
        }
        
      }else{
        colshift_flag <- 'N'
      }
    }
  }
  else{
    print("herer-----650")
  }
  ####
  new_df[1,c("X1")] <- colshift_flag
  new_df[1,c("X2")] <- colshift_val
  print('=====================>>700')
  
  return(new_df)
}

get_colshift_flag <- function(df, column , count_na) {
  
  new_df <- data.frame(matrix(ncol = 2, nrow = 0))
  
  total_count <- nrow(df)
  colshift_vals <- list()
  colshift_val <- ''
  
  #if all numeric or all string
  if((countNotNumeric(df[[column]]) == 0) | (countNotNumeric(df[[column]]) == total_count)){
    colshift_flag <- 'N'
  }else if(countNotNumeric(df[[column]]) > count_na){
    colshift_flag <- 'Y'
    #colshift_vals[['val']] <- df[which(!is.na(lapply(df[[column]], as.integer))),][[column]]
    colshift_vals[['val']] <- df[!grepl("[A-Za-z]", df[[column]]),][[column]]
    
    if(length(colshift_vals[['val']]) > 10){
      colshift_val <- paste0(noquote(paste0(head(unique(unlist(colshift_vals, use.names = FALSE)),10), collapse = ',')),' and more')
    }
    else if(length(colshift_vals[['val']]) > 0){
      colshift_val <- noquote(paste0(unique(unlist(colshift_vals, use.names = FALSE)), collapse = ','))
    }
    
  }else{
    colshift_flag <- 'N'
  }
  
  ####
  new_df[1,c("X1")] <- colshift_flag
  new_df[1,c("X2")] <- colshift_val
  
  return(new_df)
}

get_unique_count <- function(df, col){
  unique_count <- length(unique(sales_op_data[[col]]))
  return(unique_count)
}

get_missing_count <- function(df, col){
  missing_count <- sum(is.na(df[col]))
  return(missing_count)
}

get_source_table_count <- function(SCHEMA_NAME,TABLE_NAME) {
  
  soucre_count <- 0
  target_count <- 0
  result_list <- list()
  query <-  sprintf(
    "select SOURCE_TABLE_COUNT, TARGET_TABLE_COUNT, REC_COUNT_CHECK_TIME from STAGING.RECON_SPOT_QUALITY_CHECK where exclude<>'Y' AND \"SCHEMA_NAME\" = '%s' AND \"TABLE_NAME\" = '%s';",
    SCHEMA_NAME,
    TABLE_NAME
  )
  
  soucre_data <- dbGetQuery(jdbcConnection, query)
  
  if(is.data.frame(soucre_data) && nrow(soucre_data) > 0 ){
    source_count <- soucre_data[1,'SOURCE_TABLE_COUNT']
    target_count <- soucre_data[1,'TARGET_TABLE_COUNT']
    time <- soucre_data[1,'REC_COUNT_CHECK_TIME']
    target_count_variance <- round(((source_count - target_count)/source_count)*100,2)
    
    if(target_count_variance == 0){
      target_count_anomaly <- 0
    }else{
      target_count_anomaly <- 1
    }
    
    result_list <- list(source_count, target_count, target_count_variance,target_count_anomaly,time)
    
  }
  return(result_list)
}


get_data_ST <- function(dq_table, table, connection){
  
  query <- paste0(paste0(paste0("select * from ", dq_table)," where \"TABLE\" = '",table),"'")
  prev_detail <- dbGetQuery(connection, query)
  return(prev_detail)
}

get_columns_to_consider <- function(schema, table, dq_table_list, connection, source_connection, type){
  
  exclude_columns <- ''
  column_id <- ''
  query_exclude_column <- sprintf("select EXCLUDE_COLUMN from %s WHERE SCHEMA_NAME = '%s' AND TABLE_NAME = '%s'",
                                  dq_table_list, schema, table )
  #print(query_exclude_column)
  exclude_columns_df <- dbGetQuery(connection, query_exclude_column)
  
  
  query_column_id <- sprintf("select COLUMN_ID from %s WHERE SCHEMA_NAME = '%s' AND TABLE_NAME = '%s' and COLUMN_ID is not null",
                             dq_table_list, schema, table )
  #print(query_exclude_column)
  column_id_df <- dbGetQuery(connection, query_column_id)
  #print(column_id_df)
  if(is.data.frame(column_id_df) && nrow(column_id_df) > 0 ){
    column_id <- column_id_df[['COLUMN_ID']]
  }
  
  if(is.data.frame(exclude_columns_df) && nrow(exclude_columns_df) > 0 ){
    exclude_columns <- exclude_columns_df[['EXCLUDE_COLUMN']]
  }
  
  if(type == 'Query'){
    res_df <- dbGetQuery(source_connection," SELECT \"ExtractDate\"
                         , ROUND(SUM(CASE WHEN \"ForecastStage\" = '8 - Closed Won (100%)' AND SUBSTRING(QUARTER(\"ExtractDate\"), 3) = \"CloseQuarter\" THEN \"USDNetNewACV\" END )) as \"Closed\"
                         , ROUND(SUM(CASE WHEN SUBSTRING(QUARTER(\"ExtractDate\"), 3) = \"CloseQuarter\" THEN \"USDWeightedNNACV\" END)) as \"WeightedPipeline\"
                         , ROUND(SUM(CASE WHEN SUBSTRING(QUARTER(\"ExtractDate\"), 3) = \"CloseQuarter\" THEN \"USDNetNewACV\" END)) as \"TotalPipeline\"
                         , ROUND(SUM(CASE WHEN \"ForecastStage\" NOT IN ('8 - Closed Won (100%)', '8 - Closed Lost (100%)') AND SUBSTRING(QUARTER(\"ExtractDate\"), 3) = \"CloseQuarter\" THEN \"USDNetNewACV\" END)) as \"TotalOpenPipeline\"
                         , ROUND(SUM(CASE WHEN SUBSTRING(QUARTER(\"ExtractDate\"), 3) = \"CloseQuarter\" THEN \"USDRenewalACV\" END)) as \"Renewals\"
                         , ROUND(SUM(CASE WHEN \"ForecastStage\" NOT IN ('8 - Closed Won (100%)', '8 - Closed Lost (100%)') THEN \"USDRenewalACV\" END)) as \"OpenRenewals\"
                         , ROUND(SUM(CASE WHEN \"ForecastStage\" IN ('6 - Validation Completed (70%)', '7 - Deal Imminent (90%)') THEN \"USDNetNewACV\" END)) as \"MaturedPipeline\"
                         , ROUND(SUM(CASE WHEN \"ForecastStage\" IN ('1 - Opportunity (1%)','2 - Discovery (5%)','3 - Business Issue (10%)') AND SUBSTRING(QUARTER(\"ExtractDate\"), 3) = \"CloseQuarter\" THEN \"USDNetNewACV\" END)) as \"UnqualifiedPipeline\"
                         FROM \"_SYS_BIC\".\"REAL-TIME-SALES.SELFSERVICE/RPT_SALES_OPPORTUNITYITEM_SNAPSHOT_SELFSERVE\"
                         ('PLACEHOLDER' = ('$$Frequency$$', 'Daily'), 'PLACEHOLDER' = ('$$Data$$', '1'))
                         where \"ExtractDate\" = to_date(utctolocal(now(),'PST'))
                         GROUP BY \"ExtractDate\" ")
    
    colmn_list <- colnames(res_df)
  }
  else{
    if (is.na(exclude_columns) || exclude_columns == ''){
      query <- sprintf("SELECT COLUMN_NAME FROM TABLE_columns
                       WHERE SCHEMA_NAME = '%s' AND TABLE_NAME = '%s' ",
                       schema, table
      )
    }else{
      query <- sprintf("SELECT COLUMN_NAME FROM TABLE_columns
                       WHERE SCHEMA_NAME = '%s' AND TABLE_NAME = '%s'
                       AND COLUMN_NAME NOT IN (%s) ",
                       schema, table, exclude_columns
      )
    }
    
    query_new <- paste0(query, exclude_query)
    columns_df <- dbGetQuery(source_connection, query_new)
    colmn_list <- columns_df[['COLUMN_NAME']]
    
  }
  
  if(column_id != '' & !(column_id %in% colmn_list)){
    colmn_list <- c(colmn_list, column_id)
  }
  
  a <- gsub("'", '"', toString(shQuote(colmn_list)))
  
  return(a)
  }

clear_mapped_data  <- function(mapping_table, connection){
  
  ######Delete previous data from mapping table##########
  clean_query<- sprintf("DELETE FROM %s", mapping_table )
  dbSendUpdate(connection, clean_query)
  dbCommit(connection)
  #########End################
}


get_stats <- function(data_to_calculate, summary_prev_date, table_name = '', df_type = '', anomaly_calculation_period,source_connection ='' ){
  
  count_variance <- 0
  missing_variance <- 0
  unique_variance <- 0
  colshift_variance <- 0
  rowcount_anomaly <- 0
  unique_anomaly <- 0
  missing_anomaly <- 0
  colshift_anomaly <- 0
  source_count <- 0
  target_count <- 0
  target_count_variance <- 0
  target_count_anomaly <- 0
  rec_check_time <- ''
  prev_row_count <- 0
  first_col <- names(data_to_calculate[2])
  row_count <- as.numeric(nrow(data_to_calculate))
  dq_df_test_final <- as.data.frame(FALSE,)
  col_dataframe <- data.frame(matrix(ncol = 0, nrow = 0))
  score_df_final <- data.frame(matrix(ncol = 0, nrow = 0))
  dq_df_test <- data.frame(matrix(ncol = 1, nrow = 0))
  print(paste0("name===>" ,table_name))
  colnames(dq_df_test) <- c("TABLE")
  dq_df_test[1,c("TABLE")] <- c(table_name)
  print(paste0("dt type===>" ,df_type))
  
  # if(tolower(df_type) == 'scoring'){
  #   df_first_name <- sub("\\_scoring.*", "", table_name, ignore.case = TRUE)
  # } 
  
  
  df_first_name <- ifelse(tolower(df_type) == 'scoring', sub("\\_scoring.*", "", table_name, ignore.case = TRUE), '')
  
  num_attrs <- get_num_cols(data_to_calculate)
  other_attrs <- colnames(data_to_calculate)[!colnames(data_to_calculate) %in% num_attrs]
  
  if(is.data.frame(summary_prev_date) && nrow(summary_prev_date) > 1){
    prev_row_count <- as.numeric(summary_prev_date[1,"ROW_COUNT"])
  }
  
  if(prev_row_count != 0){
    count_variance <- round(((row_count - prev_row_count)/prev_row_count)*100,2)
  }else{
    count_variance <- 0 #round((row_count)*100,2)
  }
  print("here=====================740")
  print(count_variance)
  if(count_variance == 0 ){
    count_anomaly <- 0
  } else{
    count_anomaly_res <- find_anomalies(first_col, count_variance , 'COUNT_VARIANCE', table_name,dq_table, anomaly_calculation_period, df_first_name = '')
    
    if(length(count_anomaly_res) > 0){
      count_anomaly <- count_anomaly_res[[1]]
    }
    
  }
  
  anomaly <- 1
  
  print("#######Column caculation started######")
  print(format(Sys.time(), "%m/%d/%y %X"))
  
  str_DF <- get_DF_types(data_to_calculate)
  
  for(j in 1:ncol(data_to_calculate)){
    print(paste0("Column===>" , colnames(data_to_calculate)[j]))
    
    cat_attr <- if(colnames(data_to_calculate)[j] %in% other_attrs) 1 else 0
    print(paste0("cat_attr===>" , cat_attr))
    tempMatrix  <- get_summary_col(data_to_calculate,colnames(data_to_calculate)[j], cat_attr, summary_prev_date, 
                                   anomaly, table_name, dq_table, anomaly_calculation_period, df_first_name, df_type)
    
    col_dataframe <- rbind(col_dataframe, tempMatrix)
    
  }
  
  print(format(Sys.time(), "%m/%d/%y %X"))
  print("#######Column caculation ended######")
  
  n <- nrow(col_dataframe)
  dq_df_test <- do.call("rbind", replicate(n, dq_df_test, simplify = FALSE))
  
  dq_df_test_final <- cbind(dq_df_test, col_dataframe)
  dq_df_test_final <- cbind(dq_df_test_final, str_DF)
  
  dq_df_test_final <- dq_df_test_final[,-c(2)]
  #View(dq_df_test_final)
  #break
  ######Get duplictae entries######
  col_attrs_vec <- str_extract(colnames(data_to_calculate),'SYS_|_DATE')
  attr_to_consider_index <- which(is.na(col_attrs_vec))
  attr_to_consider <- colnames(data_to_calculate[attr_to_consider_index])
  
  duplicate_entry <- 0#find_duplicate_entry(data_to_calculate, attr_to_consider, tables_df[ix,'SCHEMA_NAME'],tables_df[ix,'TABLE_NAME'], dq_dups_map, jdbcConnection_ebp2)
  dq_df_test_final["DUPLICATE"] <- duplicate_entry
  ###### End ######
  
  #####Get source table detail####
  if(is.null(table_name)){
    result_count_list <- get_source_table_count(tables_df[ix,'SCHEMA_NAME'], tables_df[ix,'TABLE_NAME'])
    if(length(result_count_list) > 0){
      source_count <- result_count_list[[1]]
      target_count <- result_count_list[[2]]
      target_count_variance <- result_count_list[[3]]
      target_count_anomaly <- result_count_list[[4]]
      rec_check_time <- result_count_list[[5]]
    }
  }
  ################################
  # View(dq_df_test_final)
  # break
  ############Score###############
  print("Scoring started===")
  for(r in 1:nrow(dq_df_test_final)){
    is_not_numeric <- if(dq_df_test_final[r,"COLUMN"] %in% other_attrs) 1 else 0
    score_df_temp <- get_column_score(dq_df_test_final[r,], row_count, count_variance, count_anomaly, anomaly_calculation_period, target_count_variance, duplicate_entry, is_not_numeric)
    score_df_final <- rbind(score_df_final, score_df_temp)
    
  }
  print('herer==============')
  #View(score_df_final)
  dq_df_test_final1 <- sqldf("select * from dq_df_test_final a
                             inner join score_df_final b
                             on a.COLUMN = b.COLUMN")
  #View(dq_df_test_final1)
  ################################
  
  tryCatch(
    {
      print("*** Insert to DATASCIENCE.DATAQUALITY table ==")
      for(j in 1:nrow(dq_df_test_final)){
        
        query_dq = sprintf("INSERT INTO %s VALUES (\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',
                           \'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',
                           \'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',
                           \'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',
                           \'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',
                           \'%s\',\'%s\',\'%s\',\'%s\',\'%s\');",
                           dq_table,
                           dq_df_test_final[j,"TABLE"],
                           dq_df_test_final[j,"SYSTEM_DATE"],#date_cal, #
                           dq_df_test_final1[j,"COLUMN"],
                           row_count,
                           count_variance,
                           count_anomaly,
                           dq_df_test_final1[j,"MISSING"],
                           dq_df_test_final1[j,"MISSING_VARIANCE"],
                           dq_df_test_final1[j,"MISSING_ANOMALY"],
                           dq_df_test_final1[j,"UNIQUE"],
                           dq_df_test_final1[j,"UNIQUE_VARIANCE"],
                           dq_df_test_final1[j,"UNIQUE_ANOMALY"],
                           dq_df_test_final1[j,"COLSHIFT"],
                           dq_df_test_final1[j,"COLSHIFT_ANOMALY"],
                           dq_df_test_final1[j,"DUPLICATE"],
                           source_count,
                           target_count,
                           target_count_variance,
                           target_count_anomaly,
                           rec_check_time,
                           dq_df_test_final1[j,"COLSHIFT_VALS"],
                           dq_df_test_final1[j,"VALUE_ANOMALY"],
                           dq_df_test_final1[j,"VALUE_VARIANCE"],
                           dq_df_test_final1[j,"ACCURACY"],
                           dq_df_test_final1[j,"COMPLETENESS"],
                           dq_df_test_final1[j,"UNIQUENESS"],
                           dq_df_test_final1[j,"OVERALL_SCORE"],
                           dq_df_test_final1[j,"VALUE"],
                           dq_df_test_final1[j,"VALUE_UPPER_THRESHOLD"],
                           dq_df_test_final1[j,"VALUE_LOWER_THRESHOLD"],
                           dq_df_test_final1[j,"MISSING_UPPER_THRESHOLD"],
                           dq_df_test_final1[j,"MISSING_LOWER_THRESHOLD"],
                           dq_df_test_final1[j,"UNIQUE_UPPER_THRESHOLD"],
                           dq_df_test_final1[j,"UNIQUE_LOWER_THRESHOLD"],
                           dq_df_test_final1[j,"Data_Type"]
        )
        print(query_dq)
        dbSendUpdate(jdbcConnection_ebq, query_dq)
        dbCommit(jdbcConnection_ebq)
      }
      print(format(Sys.time(), "%m/%d/%y %X"))
      print("End of calculation =====>")
    },
    error = function(e){
      message('Caught an error!')
      print(e)
    }
  )
  print(format(Sys.time(), "%m/%d/%y %X"))
  
  print("End of calculation =====>")
}


#' Load the Dataframe
#'
#' This function loads a file as a DF. calculates the stats and save it
#' DB.
#' Any rows with duplicated row names will be dropped with the first one being
#' kepted.
#'
#' @param base_df Name to the input file
#' @param file_name Path to the input file
#' @return Void
#' @export
populate_DQ_deatil <- function(df, project_name ,type_df){
  
  tryCatch(
    {
      if(!is.null(df) && length((df)) > 1 ){
        print("DF here========>")
        summary_base_date <- as.data.frame(FALSE,)
        anomaly_calculation_period <- 5
        source_connection <- jdbcConnection_ebq
        
        df_type <- ifelse(grepl('Base', type_df,ignore.case = TRUE), 'Base', ifelse(grepl('Scoring', type_df,ignore.case = TRUE),'Scoring',''))
        
        project_name_new <- ifelse(tolower(df_type) == 'base' ,project_name, paste0(paste0(project_name,'_'), format(Sys.time(), "%Y%m%d%H%M%S")))
        
        if(tolower(df_type) == 'scoring'){
          matched_base <- get_list_of_dfs(dq_table,jdbcConnection_ebq)
          if(length(matched_base) > 0){
            summary_base_date <- get_latest_entry(dq_table, matched_base, source_connection)
          }
        }
        
        matched_base <- get_list_of_dfs(dq_table,jdbcConnection_ebq)
        print(length(matched_base))
        print(tolower(df_type))
        tryCatch({
          if((matched_base !='' && tolower(df_type) != 'base') || (matched_base == '' && tolower(df_type) == 'base') ){
            
            matched_col_list <- grepl(str_c(exclude_cols_df,  collapse="|"), colnames(df))
            exclude_cols <- colnames(df)[which(unlist(matched_col_list))]
            
            columns_to_consider_vec <- colnames(df)[!colnames(df) %in% exclude_cols]
            
            df_sub <- df[,columns_to_consider_vec]
            
            print("calculation starts---->")
            
            #convert all blank to NA
            df_sub <- df_sub %>% mutate_all(na_if,"")
            #####Stats claculation####
            
            get_stats(df_sub, summary_base_date, project_name, df_type, anomaly_calculation_period,source_connection,project_name_new)
            ####End Claculation###
            
            source("DQ_Scoring.r")
            source("DQ_Insights.r")
            populate_DQ_scores_DF(project_name_new)
            populate_DQ_insights_DF(project_name_new)
          }else{
            stop("Base project is already exist with same name.")
          }
        },
        error = function(e){
          
          message('Base project is already exist with same name.')
          print(e)
        }
        )
        
      }
    },
    error = function(e){
      message('DataFrame is not found.')
      print(e)
    }
  )
  
}