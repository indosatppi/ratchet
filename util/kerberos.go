package util

import (
	krb "gopkg.in/jcmturner/gokrb5.v7/client"
	krbconf "gopkg.in/jcmturner/gokrb5.v7/config"
	ktab "gopkg.in/jcmturner/gokrb5.v7/keytab"
)

func CreateKrbClient(user string, realm string, krb5iniFile string, keytabFile string) (*krb.Client, error) {
	keytab, err := ktab.Load(keytabFile)
	if err != nil {
		return nil, err
	}

	c, err := krbconf.Load(krb5iniFile)
	if err != nil {
		return nil, err
	}

	return krb.NewClientWithKeytab(user, realm, keytab, c), nil
}
