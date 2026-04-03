import nodemailer from "nodemailer";

export async function sendMail({ to, subject, body, attachments }) {
    const transporter = nodemailer.createTransport({
        service: "gmail",
        auth: {
            type: "OAuth2",
            user: process.env.GMAIL_USER, // e.g. you@gmail.com
            clientId: process.env.GOOGLE_CLIENT_ID,
            clientSecret: process.env.GOOGLE_CLIENT_SECRET,
            refreshToken: process.env.GOOGLE_REFRESH_TOKEN,
        },
    });

    const mailOptions = {
        from: process.env.GMAIL_USER,
        to,
        subject,
        text: body,
        attachments: attachments?.map(file => ({
            filename: file.filename,
            path: file.path,  // e.g. /opt/supabase-mcp/uploads/doubled_spice_mix.txt
        })),
    };

    const info = await transporter.sendMail(mailOptions);
    console.log("✅ Email sent:", info.response);
    return info;
}
