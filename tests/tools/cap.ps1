param([string]$Title,[string]$Out,[int]$Wait=6,[switch]$Front)
Add-Type -AssemblyName System.Drawing
Add-Type @"
using System; using System.Text; using System.Runtime.InteropServices; using System.Collections.Generic;
public class W {
  public delegate bool EP(IntPtr h, IntPtr l);
  [DllImport("user32.dll")] public static extern bool EnumWindows(EP p, IntPtr l);
  [DllImport("user32.dll")] public static extern int GetWindowText(IntPtr h, StringBuilder s, int n);
  [DllImport("user32.dll")] public static extern bool IsWindowVisible(IntPtr h);
  [DllImport("user32.dll")] public static extern bool GetWindowRect(IntPtr h, out RC r);
  [DllImport("user32.dll")] public static extern bool PrintWindow(IntPtr h, IntPtr dc, uint f);
  [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr h);
  [DllImport("user32.dll")] public static extern bool PostMessage(IntPtr h, uint m, IntPtr w, IntPtr l);
  public static string Title(IntPtr h){ var s=new StringBuilder(256); GetWindowText(h,s,256); return s.ToString(); }
  [StructLayout(LayoutKind.Sequential)] public struct RC { public int Left,Top,Right,Bottom; }
  public static IntPtr Find(string t){ IntPtr f=IntPtr.Zero; EnumWindows((h,l)=>{ if(IsWindowVisible(h)){ var s=new StringBuilder(256); GetWindowText(h,s,256); if(s.ToString().Contains(t)){f=h;return false;} } return true;},IntPtr.Zero); return f; }
}
"@
Start-Sleep -Seconds $Wait
$h=[W]::Find($Title)
if($h -eq [IntPtr]::Zero){ Write-Error "jendela '$Title' tidak ditemukan"; exit 1 }
# Bila konsol sedang dalam mode pilih teks (judul berawalan "Select"), keluaran tertahan: batalkan dengan Esc dan pastikan.
for($i=0;$i -lt 5 -and [W]::Title($h).StartsWith("Select");$i++){ [void][W]::PostMessage($h,0x100,[IntPtr]0x1B,[IntPtr]0); [void][W]::PostMessage($h,0x101,[IntPtr]0x1B,[IntPtr]0); Start-Sleep -Milliseconds 700 }
if([W]::Title($h).StartsWith("Select")){ Write-Error "konsol tetap dalam mode pilih teks"; exit 2 }
# -Front: jendela peramban di belakang jendela lain tidak digambar ulang; bawa ke depan agar isi terkini yang tertangkap.
if($Front){ [void][W]::SetForegroundWindow($h); Start-Sleep -Milliseconds 1500 }
Start-Sleep -Milliseconds 500
$r=New-Object W+RC; [void][W]::GetWindowRect($h,[ref]$r)
$w=$r.Right-$r.Left; $ht=$r.Bottom-$r.Top
$bmp=New-Object System.Drawing.Bitmap $w,$ht
$g=[System.Drawing.Graphics]::FromImage($bmp); $dc=$g.GetHdc()
[void][W]::PrintWindow($h,$dc,2); $g.ReleaseHdc($dc); $g.Dispose()
$bmp.Save($Out,[System.Drawing.Imaging.ImageFormat]::Png); $bmp.Dispose()
Write-Output "ok ${w}x${ht}"
