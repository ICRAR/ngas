#
#    (c) ICRAR, International Centre for Radio Astronomy Research
#    Copyright by ICRAR,
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

import numpy as np
import math
from cgkit import cgtypes as cgt #@UnresolvedImport
deg2rad = np.radians(1)

class Field(object):
    '''
    This class defines a structure to hold a qudrilateral spherical field. It
    provides two different kinds of initializers. One giving a center
    coordinate and an extension angle. This angle is the solid angle
    of the field on a unit sphere.
    The other initializer allows to specify the four corners.

    Restriction: The current implementation is restricted to fields which
    are aligned with the longitude great circles.
    '''


    def __init__(self, centre, fov):
        """
        Initialize the field with a centre coordinate
        INPUTS:
            centre:       (float, float): longitude and latitude
            fov:        float:  the solid angle of the field in degrees

        RETURNS:
            self
        """
        self.centreLonLat = centre

        # construct vector of centre coordintae
        self.centre = self.dircos(centre)
        self.fovd = fov
        self.fov = fov*deg2rad  # FOV is twice the angle
        self.corners = self._calcCorners_()
        self.cornersLonLat = map(lambda x: self.lonlat(x), self.corners)

        return


    def _calcCorners_(self):
        """
        Private method to calculate the corners of the field. The approach chosen
        is to calculate the lon/lat great circle planes of the centre. The lon
        great circle plane will then be rotated in N and S direction. The lat
        plane in W direction. Then the intersection vector is calculated
        and translated back into lon/lat coordinates. These yield the upper
        left corner coordinates of the field. The upper right lat is then
        lat+fov. The lower coordinates are calculated in the same way but
        starting with the S rotated lat great circle.

        NOTE: The variables for the four corners are A,B,C,D clockwise from
        upper right.

        INPUTS:
            None

        RETURNS:
            float array(4,2): lon/lat of the four corners
        """
        # calculate normal vector of GC plane along RA of centre (use north pole)
        n_ra = self._calcCross_(self.dircos([self.centreLonLat[0],90]),
                                self.dircos([self.centreLonLat[0],0]))

        tv = [self.centreLonLat[0]+90 % 360, 0] # this is the descending node
        tv = self.dircos(tv)
        # dec_rot_axis = self._rotVector_(self.centre, 90*deg2rad, tv)
        n_dec = self._calcCross_(self.centre, tv)
        # rotate...
        # normal vector plane through A and D
        n_ad = self._rotVector_(n_ra, -self.fov/2., n_dec)
        # other direction normal vector plane through B and C
        n_bc = self._rotVector_(n_ra, self.fov/2., n_dec)

        # rotate...
        # normal vector plane through A and B
        n_ab = self._rotVector_(n_dec, -self.fov/2., n_ra)
        # other direction normal vector plane through D and C
        n_dc = self._rotVector_(n_dec, self.fov/2., n_ra)

        # intersection between rotated first GC planes yields corner A
        # what is the definition of FOV exactly?? This approach will yield
        # a field which has an extension of FOV degrees measured along the
        # GC crossing the centre coordinates. At the borders of the field
        # the extension is smaller.
        a = self._calcCross_(n_ad, n_ab)
        b = self._calcCross_(n_bc, n_ab)
        c = self._calcCross_(n_bc, n_dc)
        d = self._calcCross_(n_ad, n_dc)

        self.area = self._calcArea_(n_ad, n_ab)

        # alternative method to get corners with distance of FOV
        # rotate centre vector along RA GC and then along AB GC to
        # derive corner A
#        tv = self._rotVector_(self.centre, -self.fov/2., n_ra)
#        a1 = self._rotVector_(tv, -self.fov/2., n_ab)
#        b1 = self._rotVector_(a1, self.fov, n_ab)
#
#        tv = self._rotVector_(self.centre, self.fov/2., n_dec)
#        c1 = self._rotVector_(tv, self.fov/2., n_bc )
#        d1 = self._rotVector_(c1, -self.fov, n_dc)


        return (a, b, c, d)
        #return (a1, b1, c1, d1)


    def _calcArea_(self, v1, v2):
        """
        Private method to calculate the area covered by a spherical
        quadrilateral with one corner defined by the normal vectors
        of the two intersecting great circles.

        INPUTS:
            v1, v2:  float array(3), the normal vectors

        RETURNS:
            area: float, the area given in square radians
        """
        angle = self.calcAngle(v1, v2)
        area = (4*angle - 2*np.math.pi)

        return area


    def _calcCross_(self, v1, v2):
        """
        Private method to calculate the normal vector to the plane
        spanned by two vectors

        INPUTS:
            v1, v2:   3-dim float arrays: The two vectors

        RETURNS:
            3-dim float vector: the normal vector of the plane
        """
        n_v1 = v1/np.sqrt(np.dot(v1,v1))
        n_v2 = v2/np.sqrt(np.dot(v2,v2))
        return np.cross(n_v1, n_v2).tolist()


    def _rotVector_(self, v, angle, axis):
        """
        Rotate a vector by an angle around an axis
        INPUTS:
            v:    3-dim float array
            angle:    float, the rotation angle in radians
            axis: string, 'x', 'y', or 'z'

        RETURNS:
            float array(3):  the rotated vector
        """
        # axisd = {'x':[1,0,0], 'y':[0,1,0], 'z':[0,0,1]}

        # construct quaternion and rotate...
        rot = cgt.quat()
        rot.fromAngleAxis(angle, axis)
        return list(rot.rotateVec(v))



    def calcAngle(self, v1, v2):
        """
        Calculate the angle betweenv1 with v2.
        INPUTS:
            v1: float array(n), first vector
            v2: float array(n), second vector

        RETURNS:
            float:   the angle in radians
        """
        n_v1 = v1/np.sqrt(np.dot(v1,v1))
        n_v2 = v2/np.sqrt(np.dot(v2,v2))
        angle = np.math.acos(np.dot(n_v1,n_v2))
        return angle


    def dircos(self, v):
        """
        Calculates the direction cosine of a lon/lat vector
        INPUTS:
            v:    float array(2), longitude and latitude in degrees

        RETURNS:
            float array(3), direction cosine vector
        """
        v = [v[0]*deg2rad, v[1]*deg2rad]
        vec = [np.math.cos(v[0])*np.math.cos(v[1]), np.math.sin(v[0])*np.math.cos(v[1]), np.math.sin(v[1])]
#        vec = [math.sin(v[0])*math.sin(v[1]), math.cos(v[0])*math.sin(v[1]), math.cos(v[1])]
        return vec


    def lonlat(self, v):
        """
        Calculates longitude and latitude from a dircos vector
        INPUTS:
           v:    float array(3), the dircos vector
        RETURNS
           float array(2): longitude and latitude in degrees
        """
        lat = np.math.asin(v[2])
#        lat = math.acos(v[2])
        lon = np.math.atan2(v[1], v[0])
        return [lon/deg2rad,lat/deg2rad]


if __name__ == '__main__':
    """
    This essentially is just a sophisticated test using the WALLABY
    field centres as defined by Brad Warren, calculates the corners
    and generates a JSON file containing all of the field definitions
    for the Stellarium SVMT plugin.
    """
    import sys, csv, os.path, json, copy, time

    FOV = 5.2

    f = open(sys.argv[1])
    fj = open(os.path.split(f.name)[0]+'/WallabyOb template.json')
    ob = json.load(fj)
    fj.close()

    myCsv = csv.reader(f)

    ii = 1
    obs = []
    corners = []
    with f:
        for r in myCsv:
            r1 = [float(r[0])*15, float(r[1])]
            f1 = Field(r1, FOV)
#            print f1.cornersLonLat
            new_ob = copy.deepcopy(ob)
            new_ob['id'] = "OB%05d" % ii
            new_ob['ICRAR']['observationBlock']['id'] = ii
            new_ob['ICRAR']['observationBlock']['historyStatus'] = {time.strftime('%Y-%m-%dT%H:%M:%S'):'+'}
            new_ob['ICRAR']['observationBlock']['tileCoverage'] = [f1.cornersLonLat]
            obs.append(new_ob)
            corners.append(f1.cornersLonLat)
            print obs[-1]['id']

            ii += 1

    print "%d records read" % (ii)
    f.close()

    print len(obs)
    fjo = open(os.path.split(f.name)[0]+'/WallabyObs.json', 'w')
    json.dump(obs, fjo, indent=3)
    fjo.close()
