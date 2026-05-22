import { NextResponse } from 'next/server';

export function middleware(req) {
    const token = req.cookies.get('session_token');
    const { pathname } = req.nextUrl;

    // If trying to access a protected route without a token, redirect to login
    if (!token && pathname.startsWith('/dashboard')) {
        return NextResponse.redirect(new URL('/login', req.url));
    }

    // If logged in and trying to access login page, redirect to dashboard
    if (token && pathname === '/login') {
        return NextResponse.redirect(new URL('/dashboard', req.url));
    }

    return NextResponse.next();
}