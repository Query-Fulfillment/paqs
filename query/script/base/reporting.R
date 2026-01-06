library(reactable)
library(htmlwidgets)
library(dplyr)
library(htmltools)
library(crosstalk)
library(plotly)
library(RColorBrewer)

#' @export
prettify_table <- function(df, cohort = FALSE, domain = FALSE, characteristics = FALSE,  percentage = FALSE) {
  if (cohort) {
    df <- df %>%
      mutate(
        Cohort = case_when(

          # Cohort 1 & 2 (with prefixes)
          Cohort == 'c01_any_dx_2024' ~ 'C01: Denominator',
          Cohort == 'c02_any_cancer' ~ 'C02: Any maglignant cancer',
          
          # Diagnosis-based subcohorts for patients with cancer
          Cohort == 'c03_dx_cancer_anal' ~ 'C03: Anal cancer',
          Cohort == 'c04_dx_cancer_bladder' ~ 'C04: Bladder cancer',
          Cohort == 'c05_dx_cancer_bone' ~ 'C05: Bone cancer',
          Cohort == 'c06_dx_cancer_brain' ~ 'C06: Brain cancer',
          Cohort == 'c07_dx_cancer_breast' ~ 'C07: Breast cancer',
          Cohort == 'c08_dx_cancer_cervical' ~ 'C08: Cervical cancer',
          Cohort == 'c09_dx_cancer_colorectal' ~ 'C09: Colorectal cancer',
          Cohort == 'c10_dx_cancer_endometrial' ~ 'C10: Endometrial cancer',
          Cohort == 'c11_dx_cancer_esophageal' ~ 'C11: Esophageal cancer',
          Cohort == 'c12_dx_cancer_gallbladder' ~ 'C12: Gallbladder cancer',
          Cohort == 'c13_dx_cancer_head_and_neck' ~ 'C13: Head and neck cancer',
          Cohort == 'c14_dx_cancer_hodgkins_lymphoma' ~ 'C14: Hodgkins Lymphoma',
          Cohort == 'c15_dx_cancer_kidney' ~ 'C15: Kidney cancer',
          Cohort == 'c16_dx_cancer_leukemia' ~ 'C16: Leukemia',
          Cohort == 'c17_dx_cancer_lung' ~ 'C17: Lung cancer',
          Cohort == 'c18_dx_cancer_liver' ~ 'C18: Liver cancer',
          Cohort == 'c19_dx_cancer_metastatic' ~ 'C19: Metastatic cancer',
          Cohort == 'c20_dx_cancer_nervous_system' ~ 'C20: Nervous system cancer',
          Cohort == 'c21_dx_cancer_neuroendocrine' ~ 'C21: Neuroendocrine cancer',
          Cohort == 'c22_dx_cancer_non_hodgkins_lymphoma' ~ 'C22: Non-Hodgkins Lymphoma',
          Cohort == 'c23_dx_cancer_other_unspecified' ~ 'C23: Other or unspecified cancer',
          Cohort == 'c24_dx_cancer_ovarian' ~ 'C24: Ovarian cancer',
          Cohort == 'c25_dx_cancer_pancreatic' ~ 'C25: Pancreatic cancer',
          Cohort == 'c26_dx_cancer_prostate' ~ 'C26: Prostate cancer',
          Cohort == 'c27_dx_cancer_relapse' ~ 'C27: Relapse',
          Cohort == 'c28_dx_cancer_remission' ~ 'C28: Remission',
          Cohort == 'c29_dx_cancer_sarcoma' ~ 'C29: Sarcoma',
          Cohort == 'c30_dx_cancer_skin' ~ 'C30: Skin cancer',
          Cohort == 'c31_dx_cancer_small_intestine' ~ 'C31: Small intestine cancer',
          Cohort == 'c32_dx_cancer_stomach' ~ 'C32: Stomach cancer',
          Cohort == 'c33_dx_cancer_testicular' ~ 'C33: Testicular cancer',
          Cohort == 'c34_dx_cancer_thyroid' ~ 'C34: Thyroid cancer',
          Cohort == 'c35_dx_cancer_uterine' ~ 'C35: Uterine cancer',
          
          # Treatment subcohorts for patients with cancer
          Cohort == 'c36_bisphosphonate' ~ 'C36: Bisphosphonate',
          Cohort == 'c37_chemotherapy' ~ 'C37: Chemotherapy',
          Cohort == 'c38_car_t' ~ 'C38: CAR-T cell therapy',
          Cohort == 'c39_hormone_therapy' ~ 'C39: Hormone therapy',
          Cohort == 'c40_immunotherapy' ~ 'C40: Immunotherapy',
          Cohort == 'c41_radiation' ~ 'C41: Radiation',
          Cohort == 'c42_stem_cell' ~ 'C42: Stem cell transplant',
          
          # Care setting subcohorts for patients with cancer
          Cohort == 'c43_any_cancer_2024_AV' ~ 'C43: Ambulatory Visit',
          Cohort == 'c44_any_cancer_2024_ED' ~ 'C44: Emergency Department Visit',
          Cohort == 'c45_any_cancer_2024_EI' ~ 'C45: Emergency to Inpatient Stay',
          Cohort == 'c46_any_cancer_2024_IP' ~ 'C46: Inpatient Hospital Stay',
          Cohort == 'c47_any_cancer_2024_IS' ~ 'C47: Non-Acute Institutional Stay',
          Cohort == 'c48_any_cancer_2024_OS' ~ 'C48: Observation Stay',
          Cohort == 'c49_any_cancer_2024_TH' ~ 'C49: Telehealth Visit',
          
          # Default case for any unmatched values
          TRUE ~ NA_character_
        ))
  }
  
  if (domain) {
    df <- df %>%
      mutate(
      domain = case_when(
        block == "Total" ~ "Demographics",
        block == "age_at_end" ~ "Demographics",
        block == "age_cat" ~ "Demographics",
        block == "sex_label" ~ "Demographics",
        block == "race_label" ~ "Demographics",
        block == "hispanic_label" ~ "Demographics",
        block == "year_month_of_hei" ~ "Demographics",
        block == "month_of_hei" ~ "Demographics",
        block == "year_of_hei" ~ "Demographics",
        block == "payer_cat" ~ "Demographics",
        block == "bmi_category" ~ "Vitals",
        block == "adi_quartile" ~ "Area-level Measure",
        block == "ruca_code" ~ "Area-level Measure",
        block == "state_name" ~ "Area-level Measure",
        grepl("^px_cs", block) ~ "Procedure-based Covariates",
        grepl("^dx_", block) ~ "Diagnosis-based Covariates",
        block %in% c(
          'chemotherapy',
          'bisphosphonate',
          'immunotherapy',
          'car_t',
          'stem_cell_tx',
          'hormone_therapy',
          'radiation'
        ) ~ "Cancer Treatments Covariates",
        # grepl("^rx", block) ~ "Medication-based Covariates",
        # grepl("^px", block) ~ "Procedure-based Covariates",
        # grepl("^lab", block) ~ "Lab-based Covariates",
        T ~ "Visit-based Covariates"
      )
    )
  }
  
  if (characteristics) {
    df <- df %>% mutate(
      Characteristics = case_when(
        grepl("Covariates", domain) & is.na(Characteristics) ~ "Absent",
        grepl("Covariates", domain) & Characteristics == "yes" ~ "Present",
        grepl("payer", block) & is.na(Characteristics) ~ "Missing",
        T ~ Characteristics
      )
    ) 
  }
  
  if (percentage) {
    
    df <- df %>%
      group_by(Cohort,block) %>%
      mutate(
        percentage = case_when(
          var_type == "cat" ~ round(value / sum(value) * 100, digit = 2),
          T ~ NA
        )
      )
  }
  df
}

#' @export
observable_plots_table <- function(
    df,
    attrition,
    cohort_default = NULL,
    domain_default = NULL
) {
  json_data <- jsonlite::toJSON(
    df,
    dataframe = "rows",
    pretty = FALSE,
    auto_unbox = TRUE
  )
  json_attrition <- jsonlite::toJSON(
    attrition,
    dataframe = "rows",
    pretty = FALSE,
    auto_unbox = TRUE
  )
  
  if (is.null(cohort_default)) {
    cohort_default <- unique(df$Cohort)[1]
  }
  if (is.null(domain_default)) {
    domain_default <- "Demographics"
  }
  
  tagList(
    tags$div(
      style = "padding:20px; background:white; border-radius:8px; box-shadow:0 2px 6px rgba(0,0,0,0.1);",
      tags$h4("Interactive Variable Explorer", style = "margin-top:0;"),
      
      # Filters
      tags$div(
        style = "display: flex; gap: 20px; align-items: center; margin-bottom: 20px;",
        tags$div(
          style = "display: flex; flex-direction: column; gap: 6px;",
          tags$label("Cohort:", style = "font-weight: 600; font-size: 13px; color: #374151;"),
          tags$select(
            id = "cohort-select",
            style = "padding: 8px 12px; border: 1.5px solid #e5e7eb; border-radius: 8px; font-size: 14px; background: white; color: #1f2937; cursor: pointer; transition: all 0.2s; min-width: 150px;",
            lapply(unique(df$Cohort), function(c) {
              tags$option(
                value = c,
                selected = if (c == cohort_default) "selected" else NULL,
                c
              )
            })
          )
        ),
        tags$div(
          style = "display: flex; flex-direction: column; gap: 6px;",
          tags$label("Domain:", style = "font-weight: 600; font-size: 13px; color: #374151;"),
          tags$select(
            id = "domain-select",
            style = "padding: 8px 12px; border: 1.5px solid #e5e7eb; border-radius: 8px; font-size: 14px; background: white; color: #1f2937; cursor: pointer; transition: all 0.2s; min-width: 150px;",
            lapply(unique(df$domain), function(d) {
              tags$option(
                value = d,
                selected = if (d == domain_default) "selected" else NULL,
                d
              )
            })
          )
        )
      ),
      
      # Attrition section
      tags$div(
        style = "margin-top:20px;",
        tags$h4("Attrition Table", style = "margin-bottom:10px;"),
        tags$div(id = "attrition-view")
      ),
      
      # Toggle buttons
      tags$div(
        style = "display: flex; gap: 10px; margin-top: 20px; padding: 4px; background: #f3f4f6; border-radius: 10px; width: fit-content;",
        tags$button(
          "Table",
          id = "show-table",
          style = "padding: 8px 20px; border: none; border-radius: 7px; font-size: 14px; font-weight: 600; cursor: pointer; transition: all 0.2s; background: white; color: #1f2937; box-shadow: 0 1px 3px rgba(0,0,0,0.1);"
        ),
        tags$button(
          "Plots",
          id = "show-plots",
          style = "padding: 8px 20px; border: none; border-radius: 7px; font-size: 14px; font-weight: 600; cursor: pointer; transition: all 0.2s; background: transparent; color: #6b7280;"
        )
      ),
      
      # Containers
      tags$div(
        id = "plot-grid",
        style = "
          display: none;
          grid-template-columns: repeat(2, 1fr);
          gap: 20px;
          margin-top: 20px;
          width: 100%;"
      ),
      tags$div(id = "table-view", style = "margin-top:20px; display:block;"),
      
      # Script - load TopoJSON
      tags$script(src = "https://cdnjs.cloudflare.com/ajax/libs/topojson/3.0.2/topojson.min.js"),
      
      # CSS
      tags$style(HTML(
        "
        #cohort-select:hover, #domain-select:hover {
          border-color: #3b82f6;
        }
        #cohort-select:focus, #domain-select:focus {
          outline: none;
          border-color: #3b82f6;
          box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
        }
        #show-table:hover, #show-plots:hover {
          transform: translateY(-1px);
        }
      "
      )),
      
      # Script logic
      tags$script(HTML(paste0(
        "
        (function() {
          const data = ", json_data, ";
          const attrition = ", json_attrition, ";
          const cohortSel = document.getElementById('cohort-select');
          const domainSel = document.getElementById('domain-select');
          const grid = document.getElementById('plot-grid');
          const tableDiv = document.getElementById('table-view');
          const attrDiv = document.getElementById('attrition-view');
          const btnPlots = document.getElementById('show-plots');
          const btnTable = document.getElementById('show-table');

          let usStates = null;
          fetch('https://cdn.jsdelivr.net/npm/us-atlas@3/states-albers-10m.json')
            .then(response => response.json())
            .then(us => { usStates = us; });

          function render() {
            const cohort = cohortSel.value;
            const domain = domainSel.value;

            // === ATTRITION TABLE ===
            const attrSubset = attrition.filter(d => d.Cohort === cohort);
            const attrTable = document.createElement('table');
            attrTable.style.borderCollapse = 'collapse';
            attrTable.style.width = '100%';
            attrTable.style.fontFamily = 'Signika';
            attrTable.style.fontSize = '15px';

            const attrHeader = attrTable.insertRow();
            [\"Site\",\"Step #\",\"Attrition Step\",\"# Patients\",
             \"% Retained (Prior Step)\",\"Count Diff (Prior)\",\"% Diff (Prior)\",\"% Retained (Start)\"]
             .forEach(col => {
              const th = document.createElement('th');
              th.innerText = col;
              th.style.border = '1px solid #ccc';
              th.style.padding = '6px';
              th.style.background = '#f9f9f9';
              th.style.fontWeight = '600';
              attrHeader.appendChild(th);
            });

            // Group by site, then sort steps
            const grouped = {};
            attrSubset.forEach(r => {
              if (!grouped[r.site]) grouped[r.site] = [];
              grouped[r.site].push(r);
            });

            Object.keys(grouped).forEach(site => {
              grouped[site].sort((a,b) => a.step_number - b.step_number);

              // site header row
              const siteRow = attrTable.insertRow();
              const siteCell = siteRow.insertCell();
              siteCell.colSpan = 8;
              siteCell.innerText = site;
              siteCell.style.border = '1px solid #ccc';
              siteCell.style.padding = '8px';
              siteCell.style.background = '#e9ecef';
              siteCell.style.fontWeight = 'bold';

              grouped[site].forEach(row => {
                const tr = attrTable.insertRow();
                const values = [
                  row.site,
                  row.step_number,
                  row.attrition_step,
                  row.num_pts?.toLocaleString(),
                  row.prop_retained_prior ? `${(row.prop_retained_prior*100).toFixed(1)}%` : \"\",
                  row.ct_diff_prior?.toLocaleString(),
                  row.prop_diff_prior ? `${(row.prop_diff_prior*100).toFixed(1)}%` : \"\",
                  row.prop_retained_start ? `${(row.prop_retained_start*100).toFixed(1)}%` : \"\"
                ];
                values.forEach(val => {
                  const td = tr.insertCell();
                  td.innerText = val ?? '';
                  td.style.border = '1px solid #ddd';
                  td.style.padding = '6px';
                });
              });
            });

            attrDiv.innerHTML = '';
            attrDiv.appendChild(attrTable);

            // === VARIABLE PLOTS + TABLE ===
            const subset = data.filter(d =>
              d.Cohort === cohort &&
              d.domain === domain &&
              (d.var_type === 'cat' || d.var_type === 'Total')
            );

            // === PLOTS ===
            const blocks = [...new Set(subset.map(d => d.block))].sort();
            grid.innerHTML = '';
            blocks.forEach(block => {
              const blockData = subset.filter(d => d.block === block);
              let plot;

              if (block.toLowerCase() === 'state_name' && usStates) {
                const valuemap = new Map(blockData.map(d => [d.Characteristics, d.value]));
                const values = blockData.map(d => d.value);
                const minVal = Math.min(...values);
                const maxVal = Math.max(...values);

                plot = Plot.plot({
                  projection: 'identity',
                  width: 975,
                  height: 610,
                  style: { fontSize: '12px', fontFamily: 'Signika' },
                  color: {
                    scheme: 'Blues',
                    type: 'quantize',
                    n: 9,
                    domain: [minVal, maxVal],
                    label: 'Count',
                    legend: true
                  },
                  marks: [
                    Plot.geo(topojson.feature(usStates, usStates.objects.states), Plot.centroid({
                      fill: d => valuemap.get(d.properties.name),
                      title: d => {
                        const val = valuemap.get(d.properties.name);
                        return val ? `${d.properties.name}\\n${val.toLocaleString()}` : d.properties.name;
                      },
                      tip: true
                    })),
                    Plot.geo(topojson.mesh(usStates, usStates.objects.states, (a, b) => a !== b), { stroke: 'white' })
                  ]
                });
              } else {
                plot = Plot.plot({
                  style: { fontSize: '12px', fontFamily: 'Signika' },
                  marks: [
                    Plot.barY(blockData, {x: 'Characteristics', y: 'value', fill: '#3498db'}),
                    Plot.ruleY([0])
                  ],
                  x: {tickRotate: -45},
                  y: {label: 'Count', tickFormat: '~s'},
                  height: 400,
                  marginLeft: 60,
                  marginBottom: 80,
                  title: block
                });
              }

              const wrapper = document.createElement('div');
              wrapper.style.border = '1px solid #ddd';
              wrapper.style.borderRadius = '8px';
              wrapper.style.padding = '10px';
              wrapper.style.background = 'white';
              wrapper.appendChild(plot);
              grid.appendChild(wrapper);
            });

            // === VARIABLE TABLE ===
            const table = document.createElement('table');
            table.style.borderCollapse = 'collapse';
            table.style.width = '100%';
            table.style.fontFamily = 'Signika';
            table.style.fontSize = '16px';

            const header = table.insertRow();
            ['Characteristics','Count','Percentage'].forEach(col => {
              const th = document.createElement('th');
              th.innerText = col;
              th.style.border = '1px solid #ccc';
              th.style.padding = '6px';
              th.style.background = '#f9f9f9';
              header.appendChild(th);
            });

            const blockGroups = {};
            subset.forEach(row => {
              if (!blockGroups[row.block]) blockGroups[row.block] = [];
              blockGroups[row.block].push(row);
            });

            Object.keys(blockGroups).sort().forEach(block => {
              blockGroups[block].sort((a, b) => b.value - a.value);

              const headerRow = table.insertRow();
              const headerCell = headerRow.insertCell();
              headerCell.colSpan = 3;
              headerCell.innerText = block;
              headerCell.style.border = '1px solid #ccc';
              headerCell.style.padding = '8px';
              headerCell.style.background = '#e9ecef';
              headerCell.style.fontWeight = 'bold';

              blockGroups[block].forEach(row => {
                const tr = table.insertRow();
                const values = [
                  row.Characteristics,
                  row.value?.toLocaleString(),
                  typeof row.percentage === 'number' ? `${row.percentage} %` : undefined
                ];
                values.forEach(val => {
                  const td = tr.insertCell();
                  td.innerText = val ?? '';
                  td.style.border = '1px solid #ddd';
                  td.style.padding = '6px';
                });
              });
            });

            tableDiv.innerHTML = '';
            tableDiv.appendChild(table);
          }

          cohortSel.addEventListener('change', render);
          domainSel.addEventListener('change', render);
          btnPlots.addEventListener('click', () => {
            grid.style.display = 'grid';
            tableDiv.style.display = 'none';
            btnPlots.style.background = 'white';
            btnPlots.style.color = '#1f2937';
            btnPlots.style.boxShadow = '0 1px 3px rgba(0,0,0,0.1)';
            btnTable.style.background = 'transparent';
            btnTable.style.color = '#6b7280';
            btnTable.style.boxShadow = 'none';
          });
          btnTable.addEventListener('click', () => {
            grid.style.display = 'none';
            tableDiv.style.display = 'block';
            btnTable.style.background = 'white';
            btnTable.style.color = '#1f2937';
            btnTable.style.boxShadow = '0 1px 3px rgba(0,0,0,0.1)';
            btnPlots.style.background = 'transparent';
            btnPlots.style.color = '#6b7280';
            btnPlots.style.boxShadow = 'none';
          });

          render();
        })();
      "
      )))
    )
  )
}