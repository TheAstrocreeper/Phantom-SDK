#include <jni.h>
#include <cstdint>
#include <vector>
#include "../phantom_tunnel.h"

static PhantomTunnelSession *session(jlong value) { return reinterpret_cast<PhantomTunnelSession *>(static_cast<uintptr_t>(value)); }
static void fail(JNIEnv *env) { env->ThrowNew(env->FindClass("java/io/IOException"),"Native tunnel operation failed"); }
static std::vector<uint8_t> input(JNIEnv *env,jbyteArray data) {
    if (!data) { fail(env); return {}; }
    const auto size=env->GetArrayLength(data);
    if (size<1||size>16384) { fail(env); return {}; }
    std::vector<uint8_t> bytes(size);
    env->GetByteArrayRegion(data,0,size,reinterpret_cast<jbyte *>(bytes.data()));
    return bytes;
}
extern "C" JNIEXPORT jlong JNICALL Java_io_phantom_tunnel_NativeTunnel_nPrepare(JNIEnv *env,jclass,jbyteArray data) {
    const auto bytes=input(env,data); if(env->ExceptionCheck())return 0;
    PhantomTunnelSession *handle=nullptr;
    if(phantom_tunnel_prepare(bytes.data(),bytes.size(),&handle)!=0){fail(env);return 0;}
    return static_cast<jlong>(reinterpret_cast<uintptr_t>(handle));
}
extern "C" JNIEXPORT jstring JNICALL Java_io_phantom_tunnel_NativeTunnel_nOffer(JNIEnv *env,jclass,jlong handle) {
    size_t needed=0;phantom_tunnel_offer(session(handle),nullptr,0,&needed);
    if(needed<1||needed>16384){fail(env);return nullptr;}
    std::vector<uint8_t> data(needed+1,0);
    if(phantom_tunnel_offer(session(handle),data.data(),needed,&needed)!=0){fail(env);return nullptr;}
    return env->NewStringUTF(reinterpret_cast<const char *>(data.data()));
}
extern "C" JNIEXPORT void JNICALL Java_io_phantom_tunnel_NativeTunnel_nConnect(JNIEnv *env,jclass,jlong handle,jbyteArray data,jboolean initiator) {
    const auto bytes=input(env,data);if(env->ExceptionCheck())return;
    if(phantom_tunnel_connect(session(handle),bytes.data(),bytes.size(),initiator?1:0)!=0)fail(env);
}
extern "C" JNIEXPORT jstring JNICALL Java_io_phantom_tunnel_NativeTunnel_nProbe(JNIEnv *env,jclass,jlong handle,jint bytes,jboolean initiator) {
    std::vector<uint8_t> data(2049,0);size_t written=0;
    if(bytes<1||phantom_tunnel_run_probe(session(handle),static_cast<size_t>(bytes),initiator?1:0,data.data(),2048,&written)!=0||written>2048){fail(env);return nullptr;}
    return env->NewStringUTF(reinterpret_cast<const char *>(data.data()));
}
extern "C" JNIEXPORT void JNICALL Java_io_phantom_tunnel_NativeTunnel_nClose(JNIEnv *env,jclass,jlong handle) { if(phantom_tunnel_close(session(handle))!=0)fail(env); }
extern "C" JNIEXPORT void JNICALL Java_io_phantom_tunnel_NativeTunnel_nFree(JNIEnv *,jclass,jlong handle) { phantom_tunnel_session_free(session(handle)); }

extern "C" JNIEXPORT jint JNICALL Java_io_phantom_tunnel_NativeTunnel_nProbeVersion(JNIEnv *,jclass) { return static_cast<jint>(phantom_tunnel_probe_version()); }
