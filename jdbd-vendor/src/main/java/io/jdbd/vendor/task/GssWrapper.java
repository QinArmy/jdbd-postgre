package io.jdbd.vendor.task;

import org.ietf.jgss.GSSContext;
import reactor.util.annotation.Nullable;

import javax.security.auth.login.LoginContext;

public final class GssWrapper {


    public static GssWrapper wrap(@Nullable LoginContext loginContext, GSSContext gssContext) {
        return new GssWrapper(loginContext, gssContext);
    }


    private final LoginContext loginContext;

    private final GSSContext gssContext;

    private GssWrapper(@Nullable LoginContext loginContext, GSSContext gssContext) {
        this.loginContext = loginContext;
        this.gssContext = gssContext;
    }

    @Nullable
    public final LoginContext getLoginContext() {
        return this.loginContext;
    }

    public final GSSContext getGssContext() {
        return this.gssContext;
    }
}
