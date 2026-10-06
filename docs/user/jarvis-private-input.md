# Private Jarvis input

Jarvis collects Owner secrets in a dedicated private dialog. The ordinary
workspace stays concealed during the request. Submit privately sends the secret
without adding it to your conversation. Cancel stops the secret request.

Choose Return to workspace after cancellation, a lock, or a connection loss to
continue other work. Your private Jarvis history stays locked. If the connection
is unavailable, you can still return to the workspace. To unlock again, start a
fresh Pi thread and send `/owner unlock`. Jarvis will ask for your Owner secret
privately once the connection is available.

A thread that has used Pi keeps its private-history protection after a restart.
To use another provider, start a separate thread. Changing the provider does not
make old Jarvis history public or send it to that provider.
