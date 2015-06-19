"""
Fornax:
Regridding (296, 296) 1.45967888832 072
Regridding (296, 296) 1.57747292519 080
Regridding (296, 296) 1.47762489319 088
Regridding (296, 296) 1.46148920059 095
Regridding (398, 398) 2.60201501846 103
Regridding (398, 398) 2.85688090324 111
Regridding (398, 398) 2.81434583664 118
Regridding (398, 398) 2.89099121094 126
Regridding (518, 518) 4.4000518322  139
Regridding (518, 518) 4.5449950695  147
Regridding (518, 518) 4.40605592728 154
Regridding (518, 518) 4.37225198746 162
Regridding (620, 620) 6.32177996635 170
Regridding (620, 620) 6.29985713959 177
Regridding (620, 620) 6.33518099785 185
Regridding (620, 620) 6.39884495735 193
Regridding (722, 722) 9.2360022068  200
Regridding (722, 722) 9.60528898239 208
Regridding (722, 722) 8.90700507164 216
Regridding (722, 722) 8.51990699768 223


Crab:
Regridding (296, 296) 1.45715498924
Regridding (296, 296) 1.45224785805
Regridding (296, 296) 1.45639109612
Regridding (296, 296) 1.45445990562
Regridding (398, 398) 2.60613203049
Regridding (398, 398) 2.61936998367
Regridding (398, 398) 2.81315207481
Regridding (398, 398) 2.80401492119
Regridding (518, 518) 5.41233801842
Regridding (518, 518) 5.27564787865
Regridding (518, 518) 4.73959517479
Regridding (518, 518) 5.42645192146
Regridding (620, 620) 6.91047406197
Regridding (620, 620) 7.47102308273
Regridding (620, 620) 6.7061021328
Regridding (620, 620) 8.17798304558
Regridding (722, 722) 9.1576859951
Regridding (722, 722) 9.77395296097
Regridding (722, 722) 9.15884804726
Regridding (722, 722) 9.28562092781


HerA:
Regridding (296, 296) 1.42909789085
Regridding (296, 296) 1.57087302208
Regridding (296, 296) 1.46897888184
Regridding (296, 296) 1.4295899868
Regridding (398, 398) 2.55361104012
Regridding (398, 398) 2.53458499908
Regridding (398, 398) 2.55500602722
Regridding (398, 398) 2.74981284142
Regridding (518, 518) 4.30398797989
Regridding (518, 518) 4.28300881386
Regridding (518, 518) 4.29034781456
Regridding (518, 518) 4.30084395409
Regridding (620, 620) 6.51850104332
Regridding (620, 620) 6.17562794685
Regridding (620, 620) 6.13697314262
Regridding (620, 620) 6.20811104774
Regridding (722, 722) 8.35457706451
Regridding (722, 722) 8.78116488457
Regridding (722, 722) 8.85355114937
Regridding (722, 722) 8.9790430069
"""
"""
Bar chart with three facility bars grouped by CASA tasks for easy comparison.
"""
import numpy as np
import matplotlib.pyplot as plt


n_groups = 3 # Fornax, Crab or HerA

freq1_fornax = np.array([1.46, 1.58, 1.48, 1.46])
freq1_Crab = np.array([1.46, 1.45, 1.46, 1.45])
freq1_HerA = np.array([1.43, 1.57, 1.47, 1.43])
means_freq1 = (np.mean(freq1_fornax), np.mean(freq1_Crab), np.mean(freq1_HerA))
std_freq1 = (np.std(freq1_fornax), np.std(freq1_Crab), np.std(freq1_HerA))

freq2_fornax = np.array([2.60, 2.86, 2.81, 2.89])
freq2_Crab = np.array([2.61, 2.62, 2.81, 2.80])
freq2_HerA = np.array([2.55, 2.53, 2.56, 2.75])
means_freq2 = (np.mean(freq2_fornax), np.mean(freq2_Crab), np.mean(freq2_HerA))
std_freq2 = (np.std(freq2_fornax), np.std(freq2_Crab), np.std(freq2_HerA))

freq3_fornax = np.array([4.4, 4.54, 4.41, 4.37])
freq3_Crab = np.array([5.41, 5.27, 4.73, 5.42])
freq3_HerA = np.array([4.30, 4.28, 4.29, 4.31])
means_freq3 = (np.mean(freq3_fornax), np.mean(freq3_Crab), np.mean(freq3_HerA))
std_freq3 = (np.std(freq3_fornax), np.std(freq3_Crab), np.std(freq3_HerA))

freq4_fornax = np.array([6.32, 6.30, 6.34, 6.40])
freq4_Crab = np.array([6.9, 7.47, 6.71, 8.18])
freq4_HerA = np.array([6.51, 6.17, 6.13, 6.20])
means_freq4 = (np.mean(freq4_fornax), np.mean(freq4_Crab), np.mean(freq4_HerA))
std_freq4 = (np.std(freq4_fornax), np.std(freq4_Crab), np.std(freq4_HerA))

freq5_fornax = np.array([9.24, 9.61, 8.91, 8.52])
freq5_Crab = np.array([9.16, 9.77, 9.16, 9.29])
freq5_HerA = np.array([8.35, 8.78, 8.85, 8.98])
means_freq5 = (np.mean(freq5_fornax), np.mean(freq5_Crab), np.mean(freq5_HerA))
std_freq5 = (np.std(freq5_fornax), np.std(freq5_Crab), np.std(freq5_HerA))


fig, ax = plt.subplots()

index = np.arange(n_groups)
bar_width = 0.15

opacity = 0.4
error_config = {'ecolor': '0.3'}

rects1 = plt.bar(index, means_freq1, bar_width,
                 alpha=opacity,
                 color='b',
                 yerr=std_freq1,
                 error_kw=error_config,
                 label='72~95MHz (296x296)',
                 hatch="/")

rects2 = plt.bar(index + bar_width, means_freq2, bar_width,
                 alpha=opacity,
                 color='r',
                 yerr=std_freq2,
                 error_kw=error_config,
                 label='103~126MHz (398x398)',
                 hatch="\\")

rects3 = plt.bar(index + bar_width * 2, means_freq3, bar_width,
                 alpha=opacity,
                 color='c',
                 yerr=std_freq3,
                 error_kw=error_config,
                 label='139~162MHz (518x518)',
                 hatch="-")

rects4 = plt.bar(index + bar_width * 3, means_freq4, bar_width,
                 alpha=opacity,
                 color='k',
                 yerr=std_freq4,
                 error_kw=error_config,
                 label='170~193MHz (620x620)',
                 hatch="+")

rects5 = plt.bar(index + bar_width * 4, means_freq5, bar_width,
                 alpha=opacity,
                 color='g',
                 yerr=std_freq5,
                 error_kw=error_config,
                 label='200~223MHz (722x722)',
                 hatch="x")

#plt.xlabel('Calibrator source')
plt.ylabel('Time (seconds)')
plt.title('Time of regridding (angular size = 5 degrees) around sources')
plt.xticks(index + bar_width, (" " * 25 + 'FornaxA', " " * 25 + 'Crab', " " * 25 + 'HerA'))
plt.yticks(np.arange(0, 12.1, 0.5))
plt.legend(loc = 'upper right', prop={'size':12})

plt.tight_layout()
plt.show()