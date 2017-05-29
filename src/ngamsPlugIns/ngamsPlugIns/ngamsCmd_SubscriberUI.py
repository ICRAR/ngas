import cStringIO

import pkg_resources

from ngamsLib import ngamsDbCore


###########################
#JQuery 1.9.1 libs are installed in template file
###########################

def handleCmd(srvObj, reqPropsObj, httpRef):

    """
    Handle SUBSCRIBERUI command.

    srvObj:         Reference to NG/AMS server class object (ngamsServer).

    reqPropsObj:    Request Property object to keep track of actions done
                    during the request handling (ngamsReqProps).

    httpRef:        Reference to the HTTP request handler
                    object (ngamsHttpRequestHandler).

    Returns:        Void.
    """

    f_PageHdr = pkg_resources.resource_stream(__name__, 'subscription_ui/header.html')  # @UndefinedVariable
    f_JScript = pkg_resources.resource_stream(__name__, 'subscription_ui/footer.html')  # @UndefinedVariable

    db = srvObj.getDb()
    ResultRows = db.query2("SELECT * from ngas_subscribers")


    col_name_list = ngamsDbCore.getNgasSubscribersCols().split(',')

    f = cStringIO.StringIO()

    ###########################
    #now to load up HTML pageheader
    ###########################

    for line in f_PageHdr:
        f.write(line)
    ###########################
    #now to load up Table column headers
    ###########################

    for i in range(10):
     f.write('<th>' + col_name_list[i] + '</th>\n')

    f.write('</tr>\n')
    # that is headers done
    f.write('<tr>\n')   #now for the rows
    for row in ResultRows:
        for i in range (10):
          if i != 3:                   # Most fields can be updated except subscr_id which is a PK for table- eg no of concurrent threads can be updated
           f.write('<td contentEditable>' + str(row[i]) + '</td>')
          else:
           f.write('<td>' + str(row[i]) + '</td>')
        f.write('</tr>\n')
    #  {% endfor %}
    f.write ('</table>\n')
    f.write ('</body>\n')


    ###########################
    #now to load up Java Script
    ###########################

    for line in f_JScript:
        f.write(line)
    #
    f_JScript.close()
    f_PageHdr.close()
    # f.close()
    s = f.getvalue()
    srvObj.httpReplyGen(reqPropsObj, httpRef, 200, contentType='text/html', dataRef=s)