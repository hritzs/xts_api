import { serialize } from 'cookie';

export default function handler(req, res) {
    const cookie = serialize('session_token', '', {
        httpOnly: true,
        secure: process.env.NODE_ENV !== 'development',
        expires: new Date(0), // Set to a past date to expire immediately
        sameSite: 'strict',
        path: '/',
    });

    res.setHeader('Set-Cookie', cookie);
    res.status(200).json({ message: 'Logged out' });
}