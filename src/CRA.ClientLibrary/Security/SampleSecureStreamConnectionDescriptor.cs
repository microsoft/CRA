using System;
using System.Diagnostics;
using System.IO;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Interface to define callbacks for securing TCP connections
    /// </summary>
    public class SampleSecureStreamConnectionDescriptor : ISecureStreamConnectionDescriptor
    {
        public SampleSecureStreamConnectionDescriptor()
        {
        }

        public static bool ValidateCertificate(
          object sender,
          X509Certificate certificate,
          X509Chain chain,
          SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;

            Trace.TraceError("Certificate error: {0}", sslPolicyErrors);

            // Do not allow this client to communicate with unauthenticated servers.
            return false;
        }

        public Stream CreateSecureClient(Stream stream, string serverName)
        {
            SslStream sslStream = new SslStream(stream, true,
                new RemoteCertificateValidationCallback(ValidateCertificate), null);

            try
            {
                sslStream.AuthenticateAsClient(serverName);
            }
            catch (AuthenticationException e)
            {
                Trace.TraceError("Exception: {0}", e.Message);
                if (e.InnerException != null)
                {
                    Trace.TraceError("Inner exception: {0}", e.InnerException.Message);
                }
                Trace.TraceError("Authentication failed - closing the connection.");
                sslStream.Close();
                return null;
            }
            return sslStream;
        }

        public Stream CreateSecureServer(Stream stream)
        {
            var _certificate = new X509Certificate2();

            SslStream sslStream = new SslStream(stream, true,
                new RemoteCertificateValidationCallback(ValidateCertificate), null);

            try
            {
                sslStream.AuthenticateAsServer(_certificate, false, SslProtocols.Tls, true);
            }
            catch (AuthenticationException e)
            {
                Trace.TraceError("Exception: {0}", e.Message);
                if (e.InnerException != null)
                {
                    Trace.TraceError("Inner exception: {0}", e.InnerException.Message);
                }
                Trace.TraceError("Authentication failed - closing the connection.");
                sslStream.Close();
                return null;
            }
            return sslStream;
        }

        public void TeardownSecureClient(Stream stream)
        {
            stream.Close();
        }

        public void TeardownSecureServer(Stream stream)
        {
            stream.Close();
        }
    }
}
