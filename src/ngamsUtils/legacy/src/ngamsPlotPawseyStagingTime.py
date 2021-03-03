#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2012
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#

#******************************************************************************
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      14/08/2014  Created

"""
cd /home/ngas/NGAS_roots/fe01/NGAS/bad-files
ls -rt | tail | xargs grep "Staging rate" > ~/staging_rate_Aug.txt

cd /home/ngas/NGAS_roots/fe02/NGAS/bad-files
ls -rt | tail | xargs grep "Staging rate" >> ~/staging_rate_Aug.txt
"""

import commands, numpy as np
import matplotlib.pyplot as plt


#cmd = "cat /tmp/staging_rate_Aug.txt | awk {'print $8'}"
cmd = "cat /Users/Chen/data/fe01_stage_60dyas.log | awk {'print $8'}"

re = commands.getstatusoutput(cmd)

a = []

for aa in re[1].split('\n'):
    a.append(int(aa[1:]))

x = np.array(a)

hist, bins = np.histogram(x, bins=100)
width = 0.7 * (bins[1] - bins[0])
center = (bins[:-1] + bins[1:]) / 2
fig = plt.figure()

#fig.suptitle('Pawsey staging time from 6 Aug to 13 Aug 2014', fontsize=14)
fig.suptitle('Pawsey staging time in July and August 2015', fontsize=16)
ax = fig.add_subplot(111)
ax.set_ylabel('Number of staging requests')
ax.set_xlabel('Staging completion time in seconds')
ax.set_xlim([0, 4500])

plt.bar(center, hist, align='center', width=width)
plt.show()