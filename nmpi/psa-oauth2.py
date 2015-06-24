import requests
import getpass

# In any case, the first time it has to be done through the web interface
# to choose and accept the type of access.

# base_url = r"https://accounts.google.com/o/oauth2/"
base_url = r"https://www.hbpneuromorphic.eu/"


s = requests.Session()

# 1. login button on NMPI
rNMPI1 = s.get(base_url + "login/hbp-oauth2/?next=/", allow_redirects=False, verify=False)

# 2. receives a redirect
if rNMPI1.status_code == 302:
    # Get its new destination (location)
    url = rNMPI1.headers.get('location')
    # https://services.humanbrainproject.eu/oidc/authorize?
    # 	scope=openid%20profile
    #	state=jQLERcgK1xTDHcxezNYnbmLlXhHgJmsg
    #	redirect_uri=https://neuromorphic.humanbrainproject.eu/complete/hbp-oauth2/
    #	response_type=code
    #	client_id=nmpi
    # get the exchange cookie
    cookie = rNMPI1.headers.get('set-cookie').split(";")[0]
    s.headers.update({'cookie': cookie})
    # 3. request to the provided url at HBP
    rHBP1 = s.get(url, allow_redirects=False, verify=False)

    # 4. receives a redirect to HBP login page
    if rHBP1.status_code == 302:
        # Get its new destination (location)
        url = rHBP1.headers.get('location')
        cookie = rHBP1.headers.get('set-cookie').split(";")[0]
        s.headers.update({'cookie': cookie})

        # 5. request to the provided url at HBP
        rHBP2 = s.get(url, allow_redirects=False, verify=False)

        # 6. HBP responds with the auth form
        if rHBP2.text:
            # Gather user credentials
            user = raw_input('Username:')
            pssw = getpass.getpass('Password:')

            # 7. Request to the auth service url
            formdata = {
                'j_username': user,
                'j_password': pssw,
                'submit': 'Login',
                'redirect_uri': 'https://www.hbpneuromorphic.eu/complete/hbp-oauth2/&response_type=code&client_id=nmpi'
            }
            headers = {'accept': 'application/json'}
            rNMPI2 = s.post("https://services.humanbrainproject.eu/oidc/j_spring_security_check",
                            data=formdata,
                            allow_redirects=True,
                            verify=False,
                            headers=headers)

            # check good communication
            if rNMPI2.status_code == requests.codes.ok:

                # check success address
                if rNMPI2.url == base_url:
                    # print rNMPI2.text
                    res = rNMPI2.json()
                    print res['username']
                    print res['access_token']

                # unauthorized
                else:
                    if 'error' in rNMPI2.url:
                        print "Authentication Failure: No token retrieved."
                    else:
                        print "Unhandeled error in Authentication"
            else:
                print "Communication error"
