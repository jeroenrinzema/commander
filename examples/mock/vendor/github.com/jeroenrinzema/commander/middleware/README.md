# Middleware

Commander middleware could manipulate/preform actions during various events inside commander.
Middleware has to be given to Commander via it's controller. A middleware controller is responsible for the initialization of the event subscriptions.
Check out the available [middleware events](https://github.com/jeroenrinzema/commander/blob/master/middleware.go). If you are missing a middleware event feel free to open a PR.