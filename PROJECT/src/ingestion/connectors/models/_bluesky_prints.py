from typing import Callable, Optional

from atproto import models
from atproto_client.models import blob_ref as blob_models

# ===-----------------------------------------------------------------------===#
# BlueSky API Helper Print Functions                                           #
#                                                                              #
# Author: Marc Parcerisa                                                       #
# ===-----------------------------------------------------------------------===#


def _create_print_function(print_fn: Callable[[str], None], prefix: str = ""):
    def _print(x: str):
        print_fn(f"{prefix}{x}")

    return _print


def print_actor_viewer_state(
    viewer: models.AppBskyActorDefs.ViewerState, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the viewer state.

    Args:
        viewer (models.AppBskyActorDefs.ViewerState): The viewer state to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Blocked By: {viewer.blocked_by}")
    _print(f"Blocking: {viewer.blocking}")
    _print(f"Blocking By List: {viewer.blocking_by_list}")
    _print(f"Followed By: {viewer.followed_by}")
    _print(f"Following: {viewer.following}")
    _print(f"Known Followers: {viewer.known_followers}")
    _print(f"Muted: {viewer.muted}")
    _print(f"Muted By List: {viewer.muted_by_list}")
    _print(f"Py Type: {viewer.py_type}")


def print_actor_profile_view(
    actor: models.AppBskyActorDefs.ProfileView, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the details of an actor profile view.
    Args:
        actor (models.AppBskyActorDefs.ProfileView): The actor profile view to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Did: {actor.did}")
    _print(f"Handle: {actor.handle}")
    _print(f"Associated: {actor.associated}")
    _print(f"Avatar: {actor.avatar}")
    _print(f"Created At: {actor.created_at}")
    _print(f"Description: {actor.description}")
    _print(f"Display Name: {actor.display_name}")
    _print(f"Indexed At: {actor.indexed_at}")
    _print(f"Labels: {actor.labels}")
    _print(f"Viewer:")
    print_actor_viewer_state(actor.viewer, print_fn=_print, prefix="    ")
    _print(f"Py Type: {actor.py_type}")


def print_actor_profile_view_basic(
    profile: models.AppBskyActorDefs.ProfileViewBasic,
    print_fn: Optional[Callable[[str], None]] = print,
    prefix: str = "",
):
    """
    Print the basic details of a profile.

    Args:
        profile (models.AppBskyActorDefs.ProfileViewBasic): The profile to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Did: {profile.did}")
    _print(f"Handle: {profile.handle}")
    _print(f"Associated: {profile.associated}")
    _print(f"Avatar: {profile.avatar}")
    _print(f"Created At: {profile.created_at}")
    _print(f"Display Name: {profile.display_name}")
    _print(f"Labels: {profile.labels}")
    _print(f"Viewer:")
    print_actor_viewer_state(profile.viewer, print_fn=_print, prefix="    ")
    _print(f"Py Type: {profile.py_type}")


def print_ipld_link(link: blob_models.IpldLink, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""):
    """
    Print the IPLD link details.

    Args:
        link (models.IpldLink): The IPLD link to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Link: {link.link}")


def print_blob_ref(blob_ref: blob_models.BlobRef, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""):
    """
    Print the blob reference details of an external embed.

    Args:
        blob_ref (models.AppBskyEmbedExternal.BlobRef): The blob reference to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Mime Type: {blob_ref.mime_type}")
    _print(f"Size: {blob_ref.size}")
    _print(f"Ref:")
    print_ipld_link(blob_ref.ref, print_fn=_print, prefix="    ")
    _print(f"Py Type: {blob_ref.py_type}")


def print_embed_external_external(
    view: models.AppBskyEmbedExternal.External, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the view details of an external embed.
    Args:
        view (models.AppBskyEmbedExternal.View): The view details to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Description: {view.description}")
    _print(f"Title: {view.title}")
    _print(f"Uri: {view.uri}")
    if view.thumb:
        _print(f"Thumb:")
        print_blob_ref(view.thumb, print_fn=_print, prefix="    ")
    else:
        _print("Thumb: None")
    _print(f"Py Type: {view.py_type}")


def print_embed_external_main(
    embed: models.AppBskyEmbedExternal.Main, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the embed main details of an external embed.
    Args:
        embed (models.AppBskyEmbedExternal.Main): The embed details to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"External:")
    print_embed_external_external(embed.external, print_fn=_print, prefix="    ")
    _print(f"Py Type: {embed.py_type}")


def print_embed_external_view_external(
    embed: models.AppBskyEmbedExternal.ViewExternal,
    print_fn: Optional[Callable[[str], None]] = print,
    prefix: str = "",
):
    """
    Print the embed external details of an external embed.
    Args:
        embed (models.AppBskyEmbedExternal.Main): The embed details to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Description: {embed.description}")
    _print(f"Title: {embed.title}")
    _print(f"Uri: {embed.uri}")
    _print(f"Thumb: {embed.thumb}")
    _print(f"Py Type: {embed.py_type}")


def print_embed_external_view(
    embed: models.AppBskyEmbedExternal.View, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the embed main details of an external embed.
    Args:
        embed (models.AppBskyEmbedExternal.Main): The embed details to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"External:")
    print_embed_external_view_external(embed.external, print_fn=_print, prefix="    ")
    _print(f"Py Type: {embed.py_type}")


def print_embed_image(
    image: models.AppBskyEmbedImages.Image, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the image details of an images embed.
    Args:
        image (models.AppBskyEmbedImages.Image): The image details to print.
    """
    _print = _create_print_function(print_fn, prefix)

    _print(f"Alt: {image.alt}")
    _print(f"Aspect Ratio: {image.aspect_ratio}")
    _print(f"Image:")
    print_blob_ref(image.image, print_fn=_print, prefix="    ")
    _print(f"Py Type: {image.py_type}")


def print_embed_images_main(
    embed: models.AppBskyEmbedImages.Main, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the embed main details of an images embed.
    Args:
        embed (models.AppBskyEmbedImages.Main): The embed details to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Images:")
    for j, image in enumerate(embed.images):
        _print(f" ├─ {j}:")
        print_embed_image(image, print_fn=_print, prefix=" │      ")
    _print(f"Py Type: {embed.py_type}")


def print_embed_image_view_image(
    image: models.AppBskyEmbedImages.ViewImage, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the image details of an images embed.
    Args:
        image (models.AppBskyEmbedImages.Image): The image details to print.
    """
    _print = _create_print_function(print_fn, prefix)

    _print(f"Alt: {image.alt}")
    _print(f"Aspect Ratio: {image.aspect_ratio}")
    _print(f"Fullsize: {image.fullsize}")
    _print(f"Thumb: {image.thumb}")
    _print(f"Py Type: {image.py_type}")


def print_embed_images_view(
    embed: models.AppBskyEmbedImages.View, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the embed main details of an images embed.
    Args:
        embed (models.AppBskyEmbedImages.Main): The embed details to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Images:")
    for j, image in enumerate(embed.images):
        _print(f" ├─ {j}:")
        print_embed_image_view_image(image, print_fn=_print, prefix=" │      ")
    _print(f"Py Type: {embed.py_type}")


def print_embed_video_caption(
    caption: models.AppBskyEmbedVideo.Caption, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the caption details of a video embed.
    Args:
        caption (models.AppBskyEmbedVideo.Caption): The caption details to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"File:")
    print_blob_ref(caption.file, print_fn=_print, prefix="    ")
    _print(f"Lang: {caption.lang}")
    _print(f"Py Type: {caption.py_type}")


def print_embed_video_main(
    embed: models.AppBskyEmbedVideo.Main, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the embed main details of a video embed.
    Args:
        embed (models.AppBskyEmbedVideo.Main): The embed details to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Video:")
    print_blob_ref(embed.video, print_fn=_print, prefix="    ")
    _print(f"Alt: {embed.alt}")
    _print(f"Aspect Ratio: {embed.aspect_ratio}")
    if embed.captions:
        _print(f"Captions:")
        for j, caption in enumerate(embed.captions):
            _print(f" ├─ {j}:")
            print_embed_video_caption(caption, print_fn=_print, prefix=" │      ")
    else:
        _print(f"Captions: None")
    _print(f"Py Type: {embed.py_type}")


def print_embed_video_view(
    embed: models.AppBskyEmbedVideo.View, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the embed main details of a video embed.
    Args:
        embed (models.AppBskyEmbedVideo.Main): The embed details to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Cid: {embed.cid}")
    _print(f"Playlist: {embed.playlist}")
    _print(f"Alt: {embed.alt}")
    _print(f"Aspect Ratio: {embed.aspect_ratio}")
    _print(f"Thumbnail: {embed.thumbnail}")
    _print(f"Py Type: {embed.py_type}")


def print_com_atproto_repo_strong_ref(
    ref: models.ComAtprotoRepoStrongRef.Main, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the strong reference details of a record embed.
    Args:
        ref (models.ComAtprotoRepoStrongRef.Main): The strong reference to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Cid: {ref.cid}")
    _print(f"Uri: {ref.uri}")
    _print(f"Py Type: {ref.py_type}")


def print_embed_record_view_record(
    embed: models.AppBskyEmbedRecord.ViewRecord, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the embed main details of a record embed.
    Args:
        embed (models.AppBskyEmbedRecord.Main): The embed details to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Author:")
    print_actor_profile_view_basic(embed.author, print_fn=_print, prefix="    ")
    _print(f"Cid: {embed.cid}")
    _print(f"Indexed At: {embed.indexed_at}")
    _print(f"Uri: {embed.uri}")
    _print(f"Value: {embed.value}")
    if embed.embeds:
        _print(f"Embeds:")
        for j, embed_item in enumerate(embed.embeds):
            _print(f" ├─ {j}:")
            if isinstance(embed_item, models.AppBskyEmbedImages.View):
                print_embed_images_view(embed_item, print_fn=_print, prefix=" │      ")
            elif isinstance(embed_item, models.AppBskyEmbedVideo.View):
                print_embed_video_view(embed_item, print_fn=_print, prefix=" │      ")
            elif isinstance(embed_item, models.AppBskyEmbedExternal.View):
                print_embed_external_view(embed_item, print_fn=_print, prefix=" │      ")
            elif isinstance(embed_item, models.AppBskyEmbedRecord.View):
                print_embed_record_view(embed_item, print_fn=_print, prefix=" │      ")
            elif isinstance(embed_item, models.AppBskyEmbedRecordWithMedia.View):
                print_embed_record_with_media_view(embed_item, print_fn=_print, prefix=" │      ")
            else:
                _print(f" │      Unknown embed type: {type(embed_item)}")

    else:
        _print(f"Embeds: None")
    _print(f"Labels: {embed.labels}")
    _print(f"Like Count: {embed.like_count}")
    _print(f"Quote Count: {embed.quote_count}")
    _print(f"Reply Count: {embed.reply_count}")
    _print(f"Repost Count: {embed.repost_count}")
    _print(f"Py Type: {embed.py_type}")


def print_embed_record_view(
    embed: models.AppBskyEmbedRecord.View, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the embed main details of a record embed.
    Args:
        embed (models.AppBskyEmbedRecord.Main): The embed details to print.
    """
    _print = _create_print_function(print_fn, prefix)
    if isinstance(embed.record, models.AppBskyEmbedRecord.ViewRecord):
        _print(f"Record:")
        print_embed_record_view_record(embed.record, print_fn=_print, prefix="    ")
    elif isinstance(embed.record, models.AppBskyEmbedRecord.ViewNotFound):
        _print(f"Record: Not Found")
    elif isinstance(embed.record, models.AppBskyEmbedRecord.ViewBlocked):
        _print(f"Record: Blocked")
    elif isinstance(embed.record, models.AppBskyEmbedRecord.ViewDetached):
        _print(f"Record: ViewDetached({embed.record})")
    elif isinstance(embed.record, models.AppBskyFeedDefs.GeneratorView):
        _print(f"Record: GeneratorView({embed.record})")
    elif isinstance(embed.record, models.AppBskyGraphDefs.ListView):
        _print(f"Record: ListView({embed.record})")
    elif isinstance(embed.record, models.AppBskyLabelerDefs.LabelerView):
        _print(f"Record: LabelerView({embed.record})")
    elif isinstance(embed.record, models.AppBskyGraphDefs.StarterPackViewBasic):
        _print(f"Record: StarterPackViewBasic({embed.record})")
    else:
        _print(f"Record: Unknown Type ({embed.record})")
    _print(f"Py Type: {embed.py_type}")


def print_embed_record_main(
    embed: models.AppBskyEmbedRecord.Main, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the embed main details of a record embed.
    Args:
        embed (models.AppBskyEmbedRecord.Main): The embed details to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Record:")
    print_com_atproto_repo_strong_ref(embed.record, print_fn=_print, prefix="    ")
    _print(f"Py Type: {embed.py_type}")


def print_embed_record_with_media_view(
    embed: models.AppBskyEmbedRecordWithMedia.View, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the embed main details of a record with media embed.
    Args:
        embed (models.AppBskyEmbedRecordWithMedia.Main): The embed details to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Media:")
    if isinstance(embed.media, models.AppBskyEmbedImages.View):
        print_embed_images_view(embed.media, print_fn=_print, prefix="    ")
    elif isinstance(embed.media, models.AppBskyEmbedVideo.View):
        print_embed_video_view(embed.media, print_fn=_print, prefix="    ")
    elif isinstance(embed.media, models.AppBskyEmbedExternal.View):
        print_embed_external_view(embed.media, print_fn=_print, prefix="    ")
    _print(f"Record:")
    print_embed_record_view(embed.record, print_fn=_print, prefix="    ")
    _print(f"Py Type: {embed.py_type}")


def print_embed_record_with_media_main(
    embed: models.AppBskyEmbedRecordWithMedia.Main, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the embed main details of a record with media embed.
    Args:
        embed (models.AppBskyEmbedRecordWithMedia.Main): The embed details to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Media:")
    if isinstance(embed.media, models.AppBskyEmbedImages.Main):
        print_embed_images_main(embed.media, print_fn=_print, prefix="    ")
    elif isinstance(embed.media, models.AppBskyEmbedVideo.Main):
        print_embed_video_main(embed.media, print_fn=_print, prefix="    ")
    elif isinstance(embed.media, models.AppBskyEmbedExternal.Main):
        print_embed_external_main(embed.media, print_fn=_print, prefix="    ")
    _print(f"Record:")
    print_embed_record_main(embed.record, print_fn=_print, prefix="    ")
    _print(f"Py Type: {embed.py_type}")


def print_byte_slice(
    byte_slice: models.AppBskyRichtextFacet.ByteSlice,
    print_fn: Optional[Callable[[str], None]] = print,
    prefix: str = "",
):
    """
    Print the byte slice details.

    Args:
        byte_slice (models.AppBskyRichtextFacet.ByteSlice): The byte slice to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Byte End: {byte_slice.byte_end}")
    _print(f"Byte Start: {byte_slice.byte_start}")
    _print(f"Py Type: {byte_slice.py_type}")


def print_rich_text_facet_link(
    facet: models.AppBskyRichtextFacet.Link, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the link details of a facet.

    Args:
        facet (models.AppBskyRichtextFacet.Link): The facet to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Uri: {facet.uri}")
    _print(f"Py Type: {facet.py_type}")


def print_rich_text_facet_main(
    facet: models.AppBskyRichtextFacet.Main, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the main details of a facet.

    Args:
        facet (models.AppBskyRichTextFacet.Main): The facet to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Features:")
    for j, feature in enumerate(facet.features):
        _print(f" ├─ {j}:")
        if isinstance(feature, models.AppBskyRichtextFacet.Link):
            print_rich_text_facet_link(feature, print_fn=_print, prefix=" │      ")
        else:
            _print(f" │      Unknown feature type: {type(feature)}")
            _print(f" │      Py Type: {feature.py_type}")
    _print(f"Index:")
    print_byte_slice(facet.index, print_fn=_print, prefix="    ")
    _print(f"Py Type: {facet.py_type}")


def print_post_reply_ref(
    reply: models.AppBskyFeedPost.ReplyRef, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the reply reference details.

    Args:
        reply (models.AppBskyFeedPost.ReplyRef): The reply reference to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Parent:")
    print_com_atproto_repo_strong_ref(reply.parent, print_fn=_print, prefix="    ")
    _print(f"Root:")
    print_com_atproto_repo_strong_ref(reply.root, print_fn=_print, prefix="    ")
    _print(f"Py Type: {reply.py_type}")


def print_post_record(
    record: models.AppBskyFeedPost.Record, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the details of a record.

    Args:
        record (models.AppBskyFeedDefs.Post): The record to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Created At: {record.created_at}")
    _print(f"Text: {record.text}")
    if record.embed:
        _print(f"Embed:")
        if isinstance(record.embed, models.AppBskyEmbedExternal.Main):
            print_embed_external_main(record.embed, print_fn=_print, prefix="    ")
        elif isinstance(record.embed, models.AppBskyEmbedImages.Main):
            print_embed_images_main(record.embed, print_fn=_print, prefix="    ")
        elif isinstance(record.embed, models.AppBskyEmbedVideo.Main):
            print_embed_video_main(record.embed, print_fn=_print, prefix="    ")
        elif isinstance(record.embed, models.AppBskyEmbedRecord.Main):
            print_embed_record_main(record.embed, print_fn=_print, prefix="    ")
        elif isinstance(record.embed, models.AppBskyEmbedRecordWithMedia.Main):
            print_embed_record_with_media_main(record.embed, print_fn=_print, prefix="    ")
    else:
        _print("Embed: None")
    _print(f"Entities: {record.entities}")
    if record.facets:
        _print(f"Facets:")
        for j, facet in enumerate(record.facets):
            _print(f" ├─ {j}:")
            print_rich_text_facet_main(facet, print_fn=_print, prefix=" │      ")
    else:
        _print(f"Facets: None")
    _print(f"Labels: {record.labels}")
    _print(f"Langs: {record.langs}")
    if record.reply:
        _print(f"Reply:")
        print_post_reply_ref(record.reply, print_fn=_print, prefix="    ")
    else:
        _print(f"Reply: None")
    _print(f"Tags: {record.tags}")
    _print(f"Py Type: {record.py_type}")


def print_thread_view_context(
    context: models.AppBskyFeedDefs.ThreadContext, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the details of a thread view context.

    Args:
        context (models.AppBskyFeedDefs.ThreadContext): The thread view context to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Root Author Like: {context.root_author_like}")
    _print(f"Py Type: {context.py_type}")


def print_post(
    post: models.AppBskyFeedDefs.PostView, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the details of a post.

    Args:
        post (models.AppBskyFeedDefs.PostView): The post to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Author:")
    print_actor_profile_view_basic(post.author, print_fn=_print, prefix="    ")
    _print(f"Cid: {post.cid}")
    _print(f"Embed:")
    if isinstance(post.embed, models.AppBskyEmbedExternal.View):
        print_embed_external_view(post.embed, print_fn=_print, prefix="    ")
    elif isinstance(post.embed, models.AppBskyEmbedImages.View):
        print_embed_images_view(post.embed, print_fn=_print, prefix="    ")
    elif isinstance(post.embed, models.AppBskyEmbedVideo.View):
        print_embed_video_view(post.embed, print_fn=_print, prefix="    ")
    elif isinstance(post.embed, models.AppBskyEmbedRecord.View):
        print_embed_record_view(post.embed, print_fn=_print, prefix="    ")
    elif isinstance(post.embed, models.AppBskyEmbedRecordWithMedia.View):
        print_embed_record_with_media_view(post.embed, print_fn=_print, prefix="    ")
    else:
        _print(f"    Unknown embed type: {type(post.embed)}")
    _print(f"Indexed At: {post.indexed_at}")
    _print(f"Labels: {post.labels}")
    _print(f"Like Count: {post.like_count}")
    _print(f"Py Type: {post.py_type}")
    _print(f"Quote Count: {post.quote_count}")
    _print(f"Record:")
    print_post_record(post.record, print_fn=_print, prefix="    ")
    _print(f"Reply Count: {post.reply_count}")
    _print(f"Repost Count: {post.repost_count}")
    _print(f"Threadgate: {post.threadgate}")
    _print(f"URI: {post.uri}")
    _print(f"Viewer: {post.viewer}")


def print_thread_view_post(
    thread_view: models.AppBskyFeedDefs.ThreadViewPost,
    print_fn: Optional[Callable[[str], None]] = print,
    prefix: str = "",
):
    """
    Print the details of a thread view post.

    Args:
        post (models.AppBskyFeedDefs.ThreadViewPost): The thread view post to print.
    """
    _print = _create_print_function(print_fn, prefix)
    if thread_view.parent:
        if isinstance(thread_view.parent, models.AppBskyFeedDefs.NotFoundPost):
            _print(f"Parent: Not Found")
        elif isinstance(thread_view.parent, models.AppBskyFeedDefs.BlockedPost):
            _print(f"Parent: Blocked")
        else:
            _print(f"Parent:")
            print_thread_view_post(thread_view.parent, print_fn=_print, prefix="    ")
    else:
        _print(f"Parent: None")
    _print(f"Post:")
    print_post(thread_view.post, print_fn=_print, prefix="    ")
    if thread_view.replies:
        _print(f"Replies:")
        for j, reply in enumerate(thread_view.replies):
            if isinstance(reply, models.AppBskyFeedDefs.NotFoundPost):
                _print(f" ├─ {j}: Not Found")
            elif isinstance(reply, models.AppBskyFeedDefs.BlockedPost):
                _print(f" ├─ {j}: Blocked")
            else:
                _print(f" ├─ {j}:")
                print_thread_view_post(reply, print_fn=_print, prefix=" │      ")
    else:
        _print(f"Replies: None")

    if thread_view.thread_context:
        _print(f"Thread Context:")
        print_thread_view_context(thread_view.thread_context, print_fn=_print, prefix="    ")
    else:
        _print(f"Thread Context: None")

    _print(f"Py Type: {thread_view.py_type}")


def print_like(
    like: models.AppBskyFeedGetLikes.Like, print_fn: Optional[Callable[[str], None]] = print, prefix: str = ""
):
    """
    Print the details of a like.

    Args:
        like (models.AppBskyFeedGetLikes.Like): The like to print.
    """
    _print = _create_print_function(print_fn, prefix)
    _print(f"Actor:")
    print_actor_profile_view(like.actor, print_fn=_print, prefix="    ")
    _print(f"Created At: {like.created_at}")
    _print(f"Indexed At: {like.indexed_at}")
    _print(f"Py Type: {like.py_type}")
