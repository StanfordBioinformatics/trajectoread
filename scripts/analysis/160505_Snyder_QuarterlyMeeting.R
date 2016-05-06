library(ggplot2)

job_stats_file = '/Users/pbilling/Documents/GitHub/trajectoread/scripts/analysis/160502_trajectoread_dxjob_stats.txt'
record_stats_file = '/Users/pbilling/Documents/GitHub/trajectoread/scripts/analysis/160502_trajectoread_dxrecord_stats.txt'
lanehtml_stats_file = '/Users/pbilling/Documents/GitHub/trajectoread/scripts/analysis/160504_trajectoread_lanehtml_stats.txt'

record_stats_data = read.table(record_stats_file, head=F)
names(record_stats_data) = c('Run', 'Lane', 'User', 'Lab', 'Machine', 'Mapping', 'UploadDate', 'ReleaseDate', 'ComputeTime', 'ComputeTimeHours')

hiseq4000_data = record_stats_data[record_stats_data$Machine == "Gadget" | record_stats_data$Machine == "Cooper",]
miseq_data = record_stats_data[record_stats_data$Machine == "Spenser" | record_stats_data$Machine == "Holmes",]
hiseq2000_data = record_stats_data[record_stats_data$Machine != "Gadget" & record_stats_data$Machine != "Cooper" & record_stats_data$Machine != "Spenser" & record_stats_data$Machine != "Holmes",]

hiseq4000_platform = rep('HiSeq4000', nrow(hiseq4000_data))
hiseq4000_data = cbind(hiseq4000_data, hiseq4000_platform)
names(hiseq4000_data) = c('Run', 'Lane', 'User', 'Lab', 'Machine', 'Mapping', 'UploadDate', 'ReleaseDate', 'ComputeTime', 'ComputeTimeHours', 'Platform')

hiseq2000_platform = rep('HiSeq2000', nrow(hiseq2000_data))
hiseq2000_data = cbind(hiseq2000_data, hiseq2000_platform)
names(hiseq2000_data) = c('Run', 'Lane', 'User', 'Lab', 'Machine', 'Mapping', 'UploadDate', 'ReleaseDate', 'ComputeTime', 'ComputeTimeHours', 'Platform')

miseq_platform = rep('MiSeq', nrow(miseq_data))
miseq_data = cbind(miseq_data, miseq_platform)
names(miseq_data) = c('Run', 'Lane', 'User', 'Lab', 'Machine', 'Mapping', 'UploadDate', 'ReleaseDate', 'ComputeTime', 'ComputeTimeHours', 'Platform')

record_stats_label = rbind(hiseq4000_data, hiseq2000_data, miseq_data)

#### DNAnexus Record Data ####
  
## Fig 1.A Boxplot describing compute runtimes
p <- ggplot(record_stats_label, aes(factor(Platform), ComputeTimeHours))
p + geom_boxplot(aes(fill = factor(Mapping))) + ylim(0,50) +
  labs(title = "Estimated DNAnexus Runtime", y = "Hours") +
  theme(axis.text = element_text(size = 18),
        text = element_text(size = 20),
        axis.title.x = element_blank(),
        axis.text.x = element_text(size = 20))
ggsave(file = 'EstimatedDNAnexusRuntime_mapping.png', dpi=150, width=9.6, height=5.4)

## Fig 1.A (2)
p <- ggplot(record_stats_label, aes(factor(Platform), ComputeTimeHours))
p + geom_boxplot() + geom_jitter() + ylim(0,50) +
  labs(title = "Estimated DNAnexus Runtime", y = "Hours") +
  theme(axis.text = element_text(size = 18),
        text = element_text(size = 20),
        axis.title.x = element_blank(),
        axis.text.x = element_text(size = 20))
ggsave(file = 'EstimatedDNAnexusRuntime.png', dpi=150, width=9.6, height=5.4)

## Fig 1.B Scatterplot describing compute runtimes over time
p <- ggplot(record_stats_label, aes(x=UploadDate, y=ComputeTimeHours))
p + ggtitle("DNAnexus Runtime Over Time") +
  geom_point(aes(color = factor(Platform)), size=3) + 
  geom_abline(slope=0, intercept=8) + 
  theme(axis.text = element_text(size = 18), 
        axis.title = element_text(size = 20),
        axis.text.x = element_blank(),
        legend.text = element_text(size = 18), 
        legend.title = element_text(size = 18), 
        plot.title=element_text(size=20)) + 
  xlab("Time") + 
  ylab("Hours") + 
  scale_colour_discrete(name="Platform", 
                        breaks=c("HiSeq2000", "HiSeq4000", "MiSeq"), 
                        labels=c("HiSeq 2000", "HiSeq 4000", "MiSeq"))
ggsave(file = 'EstimatedDNAnexusRuntimeOverTime.png', dpi=150, width=9.6, height=5.4)

#### DNAnexus Jobs Data ####

job_stats_data = read.table(job_stats_file, head=F)
names(job_stats_data) = c('Project', 'Machine', 'Process', 'Job_ID', 'UploadDate', 'ReleaseDate', 'ComputeTime', 'ComputeTimeHours')

hiseq4000_data = job_stats_data[job_stats_data$Machine == "GADGET" | job_stats_data$Machine == "COOPER",]
miseq_data = job_stats_data[job_stats_data$Machine == "SPENSER" | job_stats_data$Machine == "M04199",]
hiseq2000_data = job_stats_data[job_stats_data$Machine != "GADGET" & job_stats_data$Machine != "COOPER" & job_stats_data$Machine != "SPENSER" & job_stats_data$Machine != "M04199",]

hiseq4000_platform = rep('HiSeq4000', nrow(hiseq4000_data))
hiseq4000_data = cbind(hiseq4000_data, hiseq4000_platform)
names(hiseq4000_data) = c('Project', 'Machine', 'Process', 'Job_ID', 'UploadDate', 'ReleaseDate', 'ComputeTime', 'ComputeTimeHours', 'Platform')

hiseq2000_platform = rep('HiSeq2000', nrow(hiseq2000_data))
hiseq2000_data = cbind(hiseq2000_data, hiseq2000_platform)
names(hiseq2000_data) = c('Project', 'Machine', 'Process', 'Job_ID', 'UploadDate', 'ReleaseDate', 'ComputeTime', 'ComputeTimeHours', 'Platform')

miseq_platform = rep('MiSeq', nrow(miseq_data))
miseq_data = cbind(miseq_data, miseq_platform)
names(miseq_data) = c('Project', 'Machine', 'Process', 'Job_ID', 'UploadDate', 'ReleaseDate', 'ComputeTime', 'ComputeTimeHours', 'Platform')

job_stats_label = rbind(hiseq4000_data, hiseq2000_data, miseq_data)
job_stats_edit = job_stats_label[job_stats_label$Process == 'bcl2fastq' | job_stats_label$Process == 'bwa_controller' | job_stats_label$Process == 'qc_controller',]

## Fig 2. Boxplot describing DNAnexus Compute Time by process
p <- ggplot(job_stats_edit, aes(factor(Process), ComputeTimeHours))
p + geom_boxplot(aes(fill = factor(Platform))) +
  labs(title = "DNAnexus Process Runtimes", y = "Hours") +
  theme(axis.text = element_text(size = 18),
        text = element_text(size = 20),
        axis.title.x = element_blank(),
        axis.text.x = element_text(size = 20))
ggsave(file = 'DNAnexusProcessRuntime.png', dpi=150, width=9.6, height=5.4)

#### DNAnexus Lane HTML data ####

lanehtml_stats_data = read.table(lanehtml_stats_file, head=F)
names(lanehtml_stats_data) = c('Name', 'Machine', 'PF_Clusters', 'Yield_Mbases', 'Mean_Quality', 'Perc_Q30_Bases')

lanehtml_4000 = lanehtml_stats_data[lanehtml_stats_data$Machine == "GADGET" | lanehtml_stats_data$Machine == "COOPER",]
lanehtml_miseq = lanehtml_stats_data[lanehtml_stats_data$Machine == "SPENSER" | lanehtml_stats_data$Machine == "M04199",]
lanehtml_2000 = lanehtml_stats_data[lanehtml_stats_data$Machine != "GADGET" & lanehtml_stats_data$Machine != "COOPER" & lanehtml_stats_data$Machine != "SPENSER" & lanehtml_stats_data$Machine != "M04199",]

job_stats_label = rbind(hiseq4000_data, hiseq2000_data, miseq_data)

## Fig 4.A Boxplot of HiSeq 4000 PF Clusters
p <- ggplot(lanehtml_4000, aes(factor(Machine), as.numeric(PF_Clusters)))
p + geom_boxplot() +
  labs(title = "Count of Pass-filter Clusters from HiSeq 4000s", y = "Pass-filter Clusters") +
  theme(axis.text = element_text(size = 18),
        text = element_text(size = 20),
        axis.title.x = element_blank(),
        axis.text.x = element_text(size = 20))
ggsave(file = 'PFClusters_4000.png', dpi=150, width=9.6, height=5.4)

## Fig 4.A Boxplot of HiSeq 4000 PF Clusters
p <- ggplot(lanehtml_4000, aes(factor(Machine), as.numeric(PF_Clusters)))
p + geom_boxplot() + geom_jitter() +
  labs(title = "Count of Pass-filter Clusters from HiSeq 4000s", y = "Pass-filter Clusters") +
  theme(axis.text = element_text(size = 18),
        text = element_text(size = 20),
        axis.title.x = element_blank(),
        axis.text.x = element_text(size = 20))
ggsave(file = 'PFClusters_4000.png', dpi=150, width=9.6, height=5.4)

## Fig 4.B Boxplot of HiSeq 2000 PF Clusters
p <- ggplot(lanehtml_2000, aes(factor(Machine), as.numeric(PF_Clusters)))
p + geom_boxplot() + geom_jitter() +
  labs(title = "Count of Pass-filter Clusters from HiSeq 2000s", y = "Pass-filter Clusters") +
  theme(axis.text = element_text(size = 18),
        text = element_text(size = 20),
        axis.title.x = element_blank(),
        axis.text.x = element_text(size = 20))
ggsave(file = 'PFClusters_2000.png', dpi=150, width=9.6, height=5.4)

## Fig 4.C Boxplot of MiSeq PF Clusters
p <- ggplot(lanehtml_miseq, aes(factor(Machine), as.numeric(PF_Clusters)))
p + geom_boxplot() + geom_jitter() +
  labs(title = "Count of Pass-filter Clusters from MiSeqs", y = "Pass-filter Clusters") +
  theme(axis.text = element_text(size = 18),
        text = element_text(size = 20),
        axis.title.x = element_blank(),
        axis.text.x = element_text(size = 20))
ggsave(file = 'PFClusters_MiSeq.png', dpi=150, width=9.6, height=5.4)

## Fig 5.A Boxplot of HiSeq 4000 Mean Quality
p <- ggplot(lanehtml_4000, aes(factor(Machine), as.numeric(Mean_Quality)))
p + geom_boxplot() + geom_jitter() + 
  labs(title = "Mean Base Quality of HiSeq 4000s", y = "Mean Base Quality") +
  theme(axis.text = element_text(size = 18),
        text = element_text(size = 20),
        axis.title.x = element_blank(),
        axis.text.x = element_text(size = 20))
ggsave(file = 'MeanQuality_4000.png', dpi=150, width=9.6, height=5.4)

## Fig 5.B Boxplot of HiSeq 2000 Mean Quality
p <- ggplot(lanehtml_2000, aes(factor(Machine), as.numeric(Mean_Quality)))
p + geom_boxplot() + geom_jitter() + 
  labs(title = "Mean Base Quality of HiSeq 2000s", y = "Mean Base Quality") +
  theme(axis.text = element_text(size = 18),
        text = element_text(size = 20),
        axis.title.x = element_blank(),
        axis.text.x = element_text(size = 20))
ggsave(file = 'MeanQuality_2000.png', dpi=150, width=9.6, height=5.4)

## Fig 5.C Boxplot of MiSeq Mean Quality
p <- ggplot(lanehtml_miseq, aes(factor(Machine), as.numeric(Mean_Quality)))
p + geom_boxplot() + geom_jitter() + 
  labs(title = "Mean Base Quality of MiSeqs", y = "Mean Base Quality") +
  theme(axis.text = element_text(size = 18),
        text = element_text(size = 20),
        axis.title.x = element_blank(),
        axis.text.x = element_text(size = 20))
ggsave(file = 'MeanQuality_MiSeq.png', dpi=150, width=9.6, height=5.4)