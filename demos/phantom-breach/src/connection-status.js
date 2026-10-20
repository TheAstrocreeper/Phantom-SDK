// Connection notices temporarily cover activity text; recovery restores that activity.
export function connectionNotice({hadPeer,channels,state}) {
  if(!hadPeer||channels>0)return null;
  if(['failed','closed','retired'].includes(state))return {
    text:'Peer channel unavailable. Check diagnostics before leaving this room.',active:false
  };
  return {text:'Peer connection lost. Reconnecting; keep both pages open. Transfers may need Resume after recovery.',active:true};
}
