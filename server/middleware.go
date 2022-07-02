package server

import (
	"net/http"

	"github.com/tikv/pd/pkg/errs"
	"github.com/tikv/pd/server/config"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/urfave/negroni"
)

var (
	errNoClientAuthentication  = errors.New("no client certificate available for ACL evaluation")
	errBadClientAuthentication = errors.New("no matching allowed CNs or SANs in client certificate")
)

// tlsAuthMiddleware is a negroni.Handler middleware that verifies client certificates identities.
type tlsAuthMiddleware struct {
	allowedCNs  []string
	allowedSANs []string
}

// newAuthMiddleware creates a new tlsAuthMiddleware from allowed CNs and SANs defined in
// PD configuration.
func newTLSAuthMiddleware(cfg *config.Config) negroni.Handler {
	return &tlsAuthMiddleware{
		allowedCNs:  cfg.Security.CertAllowedCN,
		allowedSANs: cfg.Security.CertAllowedSAN,
	}
}

// ServeHTTP evaluates the client TLS certificate against the defined ACL and passes through the
// request to the next handler only if its identity was successfully verified. Otherwise, terminate
// the handler chain early with http.StatusUnauthorized.
func (m *tlsAuthMiddleware) ServeHTTP(rw http.ResponseWriter, req *http.Request, next http.HandlerFunc) {
	if err := m.evaluateCertificateACL(req); err != nil {
		rw.WriteHeader(http.StatusUnauthorized)
		if _, err := rw.Write([]byte(err.Error())); err != nil {
			log.Error("write failed", errs.ZapError(errs.ErrWriteHTTPBody, err))
		}
	} else {
		next.ServeHTTP(rw, req)
	}
}

// evaluateCertificateACL verifies the peer certificate CNs and SANs against the ACL. It returns nil
// for verification success and an error for verification failure.
func (m *tlsAuthMiddleware) evaluateCertificateACL(req *http.Request) error {
	// Skip ACL evaluation if no TLS context is available in the client request.
	if req.TLS == nil {
		return nil
	}

	// Client failed to supply a certificate in the request; reject always.
	if len(req.TLS.PeerCertificates) == 0 {
		return errNoClientAuthentication
	}

	for _, allowedCN := range m.allowedCNs {
		if allowedCN == req.TLS.PeerCertificates[0].Subject.CommonName {
			return nil
		}
	}

	for _, allowedSAN := range m.allowedSANs {
		// Certificate subject alternative names include both DNS names and URIs.
		if req.TLS.PeerCertificates[0].VerifyHostname(allowedSAN) == nil {
			return nil
		}
		for _, uri := range req.TLS.PeerCertificates[0].URIs {
			if allowedSAN == uri.String() {
				return nil
			}
		}
	}

	return errBadClientAuthentication
}
