import jsQR from 'jsqr';
export function createQRScanner({video,canvas,onCode,onStatus=()=>{},mediaDevices=globalThis.navigator?.mediaDevices,decode=jsQR,schedule=requestAnimationFrame,cancel=cancelAnimationFrame}) {
 let generation=0,stream=null,frame=null;
 function stop(){++generation;if(frame!==null)cancel(frame);frame=null;for(const track of stream?.getTracks()??[])track.stop();stream=null;video.pause();video.srcObject=null;}
 async function start(){stop();const run=generation;onStatus('Allow camera access to scan an invitation.');
  try{
   if(!mediaDevices?.getUserMedia)throw Error('Camera unavailable. Enter or paste the invitation below.');
   const opened=await mediaDevices.getUserMedia({video:{facingMode:{ideal:'environment'}},audio:false});
   if(run!==generation){for(const track of opened.getTracks())track.stop();return;}
   stream=opened;video.srcObject=stream;await video.play();if(run!==generation)return;
   onStatus('Point the camera at a Phantom invitation QR.');
   let last=0;const tick=at=>{if(run!==generation)return;try{if(at-last>160&&video.readyState>=2){last=at;const width=Math.min(720,video.videoWidth);if(width){canvas.width=width;canvas.height=Math.round(video.videoHeight*width/video.videoWidth);const ctx=canvas.getContext('2d',{willReadFrequently:true});ctx.drawImage(video,0,0,canvas.width,canvas.height);const pixels=ctx.getImageData(0,0,canvas.width,canvas.height);const result=decode(pixels.data,pixels.width,pixels.height);if(result?.data){onCode(result.data);stop();return;}}}frame=schedule(tick);}catch(error){stop();onStatus(error.message);}};frame=schedule(tick);
  }catch(error){if(run!==generation)return;stop();onStatus(error.name==='NotAllowedError'?'Camera permission denied. Enter or paste the invitation below.':error.message);}
 }
 return {start,stop};
}
